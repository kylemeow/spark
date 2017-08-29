/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.shuffle

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.storage.{BlockManager, ShuffleBlockFetcherIterator}
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection.ExternalSorter

/**
 * 从其他节点的 node store 请求读取之前的 shuffle 过程创建的范围 [startPartition, endPartition) 内的 partitions，所以说 Shuffle 需要 Writer 而之后的过程需要 Reader
 */
private[spark] class BlockStoreShuffleReader[K, C](
    handle: BaseShuffleHandle[K, _, C],   // 不考虑 V
    startPartition: Int,
    endPartition: Int,
    context: TaskContext,  // 任务运行的上下文环境，由 stageId、partitionId、isCompleted、isRunningLocally 等信息组成
    serializerManager: SerializerManager = SparkEnv.get.serializerManager,
    blockManager: BlockManager = SparkEnv.get.blockManager,
    mapOutputTracker: MapOutputTracker = SparkEnv.get.mapOutputTracker)
  extends ShuffleReader[K, C] with Logging {

  private val dep = handle.dependency   // TODO: 只有一个 dependency

  /** Read the combined key-values for this reduce task */
  // 核心函数，当一个 Stage 开始时的 ShuffledRDD.compute 函数调用它来读取之前的 Shuffle Write 结果
  override def read(): Iterator[Product2[K, C]] = {

    /* ShuffleBlockFetcherIterator: An iterator that fetches multiple blocks. For local blocks, it fetches from the local block
     * manager. For remote blocks, it fetches them using the provided BlockTransferService.
     *
     * This creates an iterator of (BlockID, InputStream) tuples so the caller can handle blocks
     * in a pipelined fashion as they are received.
     */

    // 获取结果 ShuffleBlockFetcher
    val blockFetcherItr = new ShuffleBlockFetcherIterator(
      context,  // context [[TaskContext]], used for metrics update
      blockManager.shuffleClient,  // shuffleClient [[ShuffleClient]] for fetching remote blocks. 返回 ExternalShuffleClient 类
      blockManager,  // blockManager [[BlockManager]] for reading local blocks
      mapOutputTracker.getMapSizesByExecutorId(handle.shuffleId, startPartition, endPartition),   // blocksByAddress list of blocks to fetch grouped by the [[BlockManagerId]].
      // Note: we use getSizeAsMb when no suffix is provided for backwards compatibility
      SparkEnv.get.conf.getSizeAsMb("spark.reducer.maxSizeInFlight", "48m") * 1024 * 1024,   // maxBytesInFlight max size (in bytes) of remote blocks to fetch at any given point.
      SparkEnv.get.conf.getInt("spark.reducer.maxReqsInFlight", Int.MaxValue))               // maxReqsInFlight max number of remote requests to fetch blocks at any given point.

    // Wrap the streams for compression and encryption based on configuration 为了压缩和加密，对 Block 流进行包装
    val wrappedStreams = blockFetcherItr.map { case (blockId, inputStream) =>
      serializerManager.wrapStream(blockId, inputStream)
    }

    val serializerInstance = dep.serializer.newInstance()  // 根据 dependency 的 serializer 来反序列化，因为之前是它给序列化的

    // Create a key/value iterator for each stream  为每个流创建一个 K,V 的 Iterator
    val recordIter = wrappedStreams.flatMap { wrappedStream =>
      // Note: the asKeyValueIterator below wraps a key/value iterator inside of a
      // NextIterator. The NextIterator makes sure that close() is called on the
      // underlying InputStream when all records have been read.
      serializerInstance.deserializeStream(wrappedStream).asKeyValueIterator
    }

    // Update the context task metrics for each record read.
    val readMetrics = context.taskMetrics().createTempShuffleReadMetrics()   // 先创建临时的 ReadMetrics
    val metricIter = CompletionIterator[(Any, Any), Iterator[(Any, Any)]](
      recordIter.map { record =>
        readMetrics.incRecordsRead(1)
        record
      },
      context.taskMetrics().mergeShuffleReadMetrics())   // 然后永久化 TODO: 具体作用呢？

    // An interruptible iterator must be used here in order to support task cancellation
    val interruptibleIter = new InterruptibleIterator[(Any, Any)](context, metricIter)   // InterruptibleIterator 可以包装一个其他的 Iterator，在遇到 context.isInterrupted 为 true 后就抛出异常而不是继续

    val aggregatedIter: Iterator[Product2[K, C]] = if (dep.aggregator.isDefined) {  // 如果需要聚合
      if (dep.mapSideCombine) {   // 如果已经在 map-side 做过了 combine，此时读取的 values 已经被 combine 函数处理过了
        val combinedKeyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, C)]]
        dep.aggregator.get.combineCombinersByKey(combinedKeyValuesIterator, context)   // 如果 value 已经做过 combine，那么就把 combiner 合并起来即可（也就是提前做了预处理）
      } else {  // 只需要 Reducer 端进行聚合
        // We don't know the value type, but also don't care -- the dependency *should*
        // have made sure its compatible w/ this aggregator, which will convert the value
        // type to the combined type C
        val keyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, Nothing)]]
        dep.aggregator.get.combineValuesByKey(keyValuesIterator, context)              // 否则如果 value 还没有做过 combine，那么就要统一把所有 value 合并起来
      }
    } else {   // 完全不需要聚合
      require(!dep.mapSideCombine, "Map-side combine without Aggregator specified!")  // 有点像 assert. 这里的意思是提供了 aggregator 才可以实现 map-side combine
      interruptibleIter.asInstanceOf[Iterator[Product2[K, C]]]
    }

    // Sort the output if there is a sort ordering defined. 判断是否需要排序
    dep.keyOrdering match {
      case Some(keyOrd: Ordering[K]) =>  // 需要排序的话
        // Create an ExternalSorter to sort the data. Note that if spark.shuffle.spill is disabled,   因为数据量太大，所以使用支持 spill 的外部排序，除非禁用了 spill 选项
        // the ExternalSorter won't spill to disk.
        val sorter =
          new ExternalSorter[K, C, C](context, ordering = Some(keyOrd), serializer = dep.serializer)   // TODO: 为什么是 [K, C, C] 呢？
        sorter.insertAll(aggregatedIter)

        // 输出统计信息
        context.taskMetrics().incMemoryBytesSpilled(sorter.memoryBytesSpilled)
        context.taskMetrics().incDiskBytesSpilled(sorter.diskBytesSpilled)
        context.taskMetrics().incPeakExecutionMemory(sorter.peakMemoryUsedBytes)

        CompletionIterator[Product2[K, C], Iterator[Product2[K, C]]](sorter.iterator, sorter.stop())   // 这个 CompletionIterator 会在 Iteration 结束时调用给定的完成方法
      case None =>   // 如果不需要排序
        aggregatedIter
    }
  }
}

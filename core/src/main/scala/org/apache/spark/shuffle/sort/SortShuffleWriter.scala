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

package org.apache.spark.shuffle.sort

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.{BaseShuffleHandle, IndexShuffleBlockResolver, ShuffleWriter}
import org.apache.spark.storage.ShuffleBlockId
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.ExternalSorter

private[spark] class SortShuffleWriter[K, V, C](
    shuffleBlockResolver: IndexShuffleBlockResolver,    // 可以根据 shuffleId 和 mapId 得到 data 和 index 文件，即逻辑块和实际物理地址的对应关系
    handle: BaseShuffleHandle[K, V, C],   // 有 shuffleId、numMaps、dependency 等各种信息，用来提示是以什么方式 Shuffle 的
    mapId: Int,   // TODO: 但是为什么调用是传入的是 partitionId 呢？一个 partition 一个 map？
    context: TaskContext)    // 主要有 stageId 和 partitionId
  extends ShuffleWriter[K, V] with Logging {

  private val dep = handle.dependency

  private val blockManager = SparkEnv.get.blockManager

  private var sorter: ExternalSorter[K, V, _] = null

  // Are we in the process of stopping? Because map tasks can call stop() with success = true
  // and then call stop() with success = false if they get an exception, we want to make sure
  // we don't try deleting files, etc twice.
  private var stopping = false

  private var mapStatus: MapStatus = null

  private val writeMetrics = context.taskMetrics().shuffleWriteMetrics

  /** Write a bunch of records to this task's output */
  // ShuffleMapTask 对本方法的用法：
  // writer = manager.getWriter[Any, Any](dep.shuffleHandle, partitionId, context)   // TODO: 定义是 mapId，这里怎么是 partitionId 呢？都是 Int 数字，也许这里相等？这个 partitionId 和下面的 partition 对象的联系？
  // writer.write(rdd.iterator(partition, context).asInstanceOf[Iterator[_ <: Product2[Any, Any]]])   // taskContext 里面可以更新一些 Metrics 统计信息等等

  // 这个方法很重要，具体的使用 RDD Iterator 写入的过程
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    // 如果指定的 map-side combine，那么必须同时指定 aggregator.
    // 如果有 aggregator，那么需要传入 keyOrdering，这样排序后归并更快
    sorter = if (dep.mapSideCombine) {
      require(dep.aggregator.isDefined, "Map-side combine without Aggregator specified!")
      new ExternalSorter[K, V, C](
        context, dep.aggregator, Some(dep.partitioner), dep.keyOrdering, dep.serializer)    // combine 的话需要排序才可以快速
    } else {
      // In this case we pass neither an aggregator nor an ordering to the sorter, because we don't
      // care whether the keys get sorted in each partition; that will be done on the reduce side
      // if the operation being run is sortByKey.
      new ExternalSorter[K, V, V](   // 如果没有指定 aggregator，那么 V 和 C 是相等的。传入参数构建所需的 ExternalSorter
        context, aggregator = None, Some(dep.partitioner), ordering = None, dep.serializer)
    }
    sorter.insertAll(records)  // 这是在排序

    // Don't bother including the time to open the merged output file in the shuffle write time,
    // because it just opens a single file, so is typically too fast to measure accurately
    // (see SPARK-3570).
    // 获取临时数据文件，准备写入了
    val output = shuffleBlockResolver.getDataFile(dep.shuffleId, mapId)  // 只是得到个文件名而已
    val tmp = Utils.tempFileWith(output)  // 先创建一个临时文件
    try {
      val blockId = ShuffleBlockId(dep.shuffleId, mapId, IndexShuffleBlockResolver.NOOP_REDUCE_ID)   // 获取文件名 blockId
      val partitionLengths = sorter.writePartitionedFile(blockId, tmp)    // 让 sorter 开始写临时文件，这是核心的部分
      shuffleBlockResolver.writeIndexFileAndCommit(dep.shuffleId, mapId, partitionLengths, tmp)    // 然后把临时文件 commit
      mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths)   // 根据结果返回 MapStatus，这就完成了一次 ShuffleMapTask. 参数含义是 loc: BlockManagerId, uncompressedSizes: Array[Long]) TODO: shuffleServerId 是哪里赋值的
    } finally {
      if (tmp.exists() && !tmp.delete()) {
        logError(s"Error while deleting temp file ${tmp.getAbsolutePath}")
      }
    }
  }

  /** Close this writer, passing along whether the map completed */
  override def stop(success: Boolean): Option[MapStatus] = {
    try {
      if (stopping) {    // 如果正处于 stopping 状态，就不参与了
        return None
      }
      stopping = true
      if (success) {
        return Option(mapStatus)
      } else {
        return None
      }
    } finally {
      // Clean up our sorter, which may have its own intermediate files
      if (sorter != null) {
        val startTime = System.nanoTime()
        sorter.stop()
        writeMetrics.incWriteTime(System.nanoTime - startTime)
        sorter = null
      }
    }
  }
}

private[spark] object SortShuffleWriter {
  def shouldBypassMergeSort(conf: SparkConf, dep: ShuffleDependency[_, _, _]): Boolean = {
    // We cannot bypass sorting if we need to do map-side aggregation.
    if (dep.mapSideCombine) {
      require(dep.aggregator.isDefined, "Map-side combine without Aggregator specified!")
      false
    } else {
      val bypassMergeThreshold: Int = conf.getInt("spark.shuffle.sort.bypassMergeThreshold", 200)
      dep.partitioner.numPartitions <= bypassMergeThreshold
    }
  }
}

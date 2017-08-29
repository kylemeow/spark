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

package org.apache.spark.rdd

import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.serializer.Serializer

private[spark] class ShuffledRDDPartition(val idx: Int) extends Partition {  // ShuffledRDD 是一种 Partition，可以 Serializable，有 index 可以查看 ID
  override val index: Int = idx   // The partition's index within its parent RDD // TODO: 注意是 parent RDD

  override def hashCode(): Int = index  // index 即为 hashCode

  override def equals(other: Any): Boolean = super.equals(other)
}

/**
 * :: DeveloperApi ::
 * The resulting RDD from a shuffle (e.g. repartitioning of data).  shuffle 操作以后生成这个类型的 RDD
 * @param prev prev 指向 parent RDD
 * @param part RDD 而 part 就是划分这个 RDD 的 partitioner
 * @tparam K the key class.
 * @tparam V the value class.
 * @tparam C the combiner class.
 */
// TODO: Make this return RDD[Product2[K, C]] or have some way to configure mutable pairs
@DeveloperApi   // DeveloperApi 随时可以更改，小版本也可能增删
// 调用例子如 RDD.scala： new ShuffledRDD[Int, T, T](mapPartitionsWithIndex(distributePartition), new HashPartitioner(numPartitions))
class ShuffledRDD[K: ClassTag, V: ClassTag, C: ClassTag]
(@transient var prev: RDD[_ <: Product2[K, V]], part: Partitioner)   // TODO: 为什么不需要序列化
  extends RDD[(K, C)](prev.context, Nil) {  // SparkContext 为 prev.context 继续用；Dependencies 为 Nil，无可依赖

  private var userSpecifiedSerializer: Option[Serializer] = None   // 可选参数，通常为 None

  private var keyOrdering: Option[Ordering[K]] = None

  private var aggregator: Option[Aggregator[K, V, C]] = None

  private var mapSideCombine: Boolean = false

  /** Set a serializer for this RDD's shuffle, or null to use the default (spark.serializer) */
  def setSerializer(serializer: Serializer): ShuffledRDD[K, V, C] = {
    this.userSpecifiedSerializer = Option(serializer)
    this  // 返回 this 从而支持链式调用
  }

  /** Set key ordering for RDD's shuffle. */
  def setKeyOrdering(keyOrdering: Ordering[K]): ShuffledRDD[K, V, C] = {
    this.keyOrdering = Option(keyOrdering)
    this
  }

  /** Set aggregator for RDD's shuffle. */
  def setAggregator(aggregator: Aggregator[K, V, C]): ShuffledRDD[K, V, C] = {
    this.aggregator = Option(aggregator)
    this
  }

  /** Set mapSideCombine flag for RDD's shuffle. */
  def setMapSideCombine(mapSideCombine: Boolean): ShuffledRDD[K, V, C] = {
    this.mapSideCombine = mapSideCombine
    this
  }

  // 得到这个 RDD 的 dependencies
  override def getDependencies: Seq[Dependency[_]] = {
    val serializer = userSpecifiedSerializer.getOrElse {  // 这个 Scala 用法很简洁，如果 Option 为 Nil 就使用下面默认提供的，否则使用用户提供的，避免 if else 判断
      val serializerManager = SparkEnv.get.serializerManager  // 一般都是从 SparkEnv.get.xxxManager 得到 xxx

      // 针对是否使用 mapSideCombine 进行判断
      if (mapSideCombine) {
        // 传入的参数是让 getSerializer 判断 Kryo Serializer 是否支持对这两种类的序列化，如果支持就用 Kryo，不然就用默认的
        serializerManager.getSerializer(implicitly[ClassTag[K]], implicitly[ClassTag[C]])     // 如果前面有声明对应类型的 implicit 变量，这里会自动取得它的值，而不管它是谁。TODO: 可是为什么要这样用呢？ V 为什么不管？
      } else {
        serializerManager.getSerializer(implicitly[ClassTag[K]], implicitly[ClassTag[V]])     // TODO: 为什么不用 mapSideCombine 就是 K, V 了呢？
      }
    }

    // 对应编程时调用的 dependencies，返回值类似 Seq[org.apache.spark.Dependency[_]] = List(org.apache.spark.ShuffleDependency@40c6d611)
    List(new ShuffleDependency(prev, part, serializer, keyOrdering, aggregator, mapSideCombine))
  }

  override val partitioner = Some(part)

  override def getPartitions: Array[Partition] = {
    Array.tabulate[Partition](part.numPartitions)(i => new ShuffledRDDPartition(i))  // Array.tabulate 生成从 0 ~ numPartitions-1 的 index 并依次传入函数，不用 for 循环了。ShuffledRDDPartition 根据 id 得到对应 partition
  }

  override protected def getPreferredLocations(partition: Partition): Seq[String] = {
    val tracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]   //  mapOutputTracker keeps track of the location of the map output of a stage
    val dep = dependencies.head.asInstanceOf[ShuffleDependency[K, V, C]]
    tracker.getPreferredLocationsForShuffle(dep, partition.index)
  }

  // To compute a given partition，也就是读取上一个阶段的结果
  override def compute(split: Partition, context: TaskContext): Iterator[(K, C)] = {   // 调用 reader 返回的是一个 Iterator
    val dep = dependencies.head.asInstanceOf[ShuffleDependency[K, V, C]]
    SparkEnv.get.shuffleManager.getReader(dep.shuffleHandle, split.index, split.index + 1, context)
      .read()
      .asInstanceOf[Iterator[(K, C)]]  // TODO: 何时 K, V，何时又 K, C？
  }

  override def clearDependencies() {
    super.clearDependencies()
    prev = null
  }
}

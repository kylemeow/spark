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

import org.apache.spark.{ShuffleDependency, TaskContext}

/**
 * Pluggable interface for shuffle systems. A ShuffleManager is created in SparkEnv on the driver
 * and on each executor, based on the spark.shuffle.manager setting. The driver registers shuffles
 * with it, and executors (or tasks running locally in the driver) can ask to read and write data.
 *
 * NOTE: this will be instantiated by SparkEnv so its constructor can take a SparkConf and
 * boolean isDriver as parameters.
 */
private[spark] trait ShuffleManager {

  /**
   * 注册 Shuffle，并且返回一个可以传递给 task 的句柄
   */
  def registerShuffle[K, V, C](
      shuffleId: Int,
      numMaps: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle

  /** 为一个特定的 partition 得到一个 writer，然后由 executors 在 map 任务阶段调用它。非常重要，Map Task 用 writer，Reduce Task 用 reader */
  def getWriter[K, V](handle: ShuffleHandle, mapId: Int, context: TaskContext): ShuffleWriter[K, V]

  /**
   * 为一个区间的 reduce partitions 得到一个 reader 区间为 [startPartition to endPartition-1]. Executor 会在 reduce 阶段调用它
   */
  def getReader[K, C](
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext): ShuffleReader[K, C]

  /**
   * Remove a shuffle's metadata from the ShuffleManager.
   * @return true if the metadata removed successfully, otherwise false.
   */
  def unregisterShuffle(shuffleId: Int): Boolean

  /**
   * 返回一个根据 block 坐标即可得到 shuffle block 数据的解析器
   */
  def shuffleBlockResolver: ShuffleBlockResolver

  /** Shut down this ShuffleManager. */
  def stop(): Unit
}

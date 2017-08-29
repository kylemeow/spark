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

package org.apache.spark.scheduler

import java.lang.management.ManagementFactory
import java.nio.ByteBuffer
import java.util.Properties

import scala.language.existentials

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.shuffle.ShuffleWriter

/**
 * ShuffleMapTask 的职责是根据 ShuffleDependency 中提供的 partitioner 把 RDD 分为若干 buckets
 * A ShuffleMapTask executes the task and divides the task output to multiple buckets (based on the task's partitioner)
 *
 * See [[org.apache.spark.scheduler.Task]] for more information.
 *
 * @param stageId 任务所属的 stage id
 * @param stageAttemptId attempt id of the stage this task belongs to
 * @param taskBinary RDD 和 ShuffleDependency 序列化后的广播变量版本. 反序列化后的形式是 (RDD[_], ShuffleDependency[_, _, _])
 * @param partition partition of the RDD this task is associated with
 * @param locs preferred task execution locations for locality scheduling  根据数据来分配计算，而非根据计算而移动数据
 * @param metrics a `TaskMetrics` that is created at driver side and sent to executor side.
 * @param localProperties copy of thread-local properties set by the user on the driver side.
 *
 * The parameters below are optional: Optional 的选项就用 Option
 * @param jobId id of the job this task belongs to
 * @param appId id of the app this task belongs to
 * @param appAttemptId attempt id of the app this task belongs to
 */

// taskBinary 在 DAGScheduler 中创建，来源是：
// JavaUtils.bufferToArray(closureSerializer.serialize((stage.rdd, stage.shuffleDep): AnyRef))
// taskBinary = sc.broadcast(taskBinaryBytes)

// ShuffleMapTask 由 DAGScheduler 调用
private[spark] class ShuffleMapTask(
    stageId: Int,   // 任务所属的 stageiD
    stageAttemptId: Int,
    taskBinary: Broadcast[Array[Byte]],   // 这里面保存着 RDD 和 ShuffleDependency 序列化后的广播版本
    partition: Partition,
    @transient private var locs: Seq[TaskLocation],
    metrics: TaskMetrics,    // 统计消耗时间、内存、CPU 占用等一些信息
    localProperties: Properties,    // 用户在 Driver 端设置的一些属性，是 thread-local 的

    jobId: Option[Int] = None,    // None 和 Some 对应 Option；而 Nil 是 List 的，不要混淆
    appId: Option[String] = None,
    appAttemptId: Option[String] = None)
  extends Task[MapStatus](stageId, stageAttemptId, partition.index, metrics, localProperties, jobId,
    appId, appAttemptId)
  with Logging {

  /** A constructor used only in TEST suites. This does not require passing in an RDD. */
  def this(partitionId: Int) {
    this(0, 0, null, new Partition { override def index: Int = 0 }, null, null, new Properties)
  }

  @transient private val preferredLocs: Seq[TaskLocation] = {
    if (locs == null) Nil else locs.toSet.toSeq   // Nil 可以放心地用来表示 List/Seq 的空元素
  }

  // 用来启动任务的核心函数：由 Scheduler 调用 runTask，返回值 MapStatus 会返回给 Scheduler，然后传给 reduce 部分
  override def runTask(context: TaskContext): MapStatus = {
    // 统计时间所需的相关变量，以后 taskMetrics 用得到
    val threadMXBean = ManagementFactory.getThreadMXBean
    val deserializeStartTime = System.currentTimeMillis()
    val deserializeStartCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
      threadMXBean.getCurrentThreadCpuTime
    } else 0L

    // 根据广播变量把 RDD 反序列化出来。序列化时用的是 closureSerializer，那么反序列化同样也用它
    val ser = SparkEnv.get.closureSerializer.newInstance()  // closureSerializer 主要适用于 RDD 等序列化，而普通的 serializer 则是对数据进行序列化
    val (rdd, dep) = ser.deserialize[(RDD[_], ShuffleDependency[_, _, _])](
      ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)  // 传入当前 Thread 的 ClassLoader，用来反序列化给定的 ByteBuffer 二进制数据

    // 时间统计结束，存入相关变量
    _executorDeserializeTime = System.currentTimeMillis() - deserializeStartTime
    _executorDeserializeCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
      threadMXBean.getCurrentThreadCpuTime - deserializeStartCpuTime
    } else 0L

    // 重点在这里，这里从 shuffleManager 中获取一个 ShuffleWriter 然后写入 rdd，之后 ShuffledRDD 同样通过 shuffleManager 获取一个 Reader 来读取数据
    var writer: ShuffleWriter[Any, Any] = null   // TODO: 注意 [Any, Any] 和 [_, _] 的区别
    try {
      val manager = SparkEnv.get.shuffleManager   // 先得到 ShuffleManager（之前分为 Hash 和 Sort，现在已经合并），再得到 writer 用来写数据（map 阶段是 writer 写数据，reduce 阶段是 reader 读数据）
      writer = manager.getWriter[Any, Any](dep.shuffleHandle, partitionId, context)   // TODO: 定义是 mapId，这里怎么是 partitionId 呢？都是 Int 数字，也许这里相等？
      writer.write(rdd.iterator(partition, context).asInstanceOf[Iterator[_ <: Product2[Any, Any]]])   // taskContext 里面可以更新一些 Metrics 统计信息等等
      writer.stop(success = true).get   // 关闭 writer，并告知它写入成功，得知 MapStatus：Result returned by a ShuffleMapTask to a scheduler. Includes the block manager address that the task ran on as well as the sizes of outputs for each reducer, for passing on to the reduce tasks.
    } catch {
      case e: Exception =>
        try {
          if (writer != null) {
            writer.stop(success = false)   // 出现异常，关闭 writer，并告知失败了
          }
        } catch {
          case e: Exception =>
            log.debug("Could not stop writer", e)
        }
        throw e
    }
  }

  override def preferredLocations: Seq[TaskLocation] = preferredLocs

  override def toString: String = "ShuffleMapTask(%d, %d)".format(stageId, partitionId)
}

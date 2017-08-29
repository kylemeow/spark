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

import java.io._

import com.google.common.io.ByteStreams

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.io.NioBufferedFileInputStream
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.shuffle.IndexShuffleBlockResolver.NOOP_REDUCE_ID
import org.apache.spark.storage._
import org.apache.spark.util.Utils

/**
 * Create and maintain the shuffle blocks' mapping between logic block and physical file location.
 * Data of shuffle blocks from the same map task are stored in a single consolidated data file.
 * The offsets of the data blocks in the data file are stored in a separate index file.
 *
 * We use the name of the shuffle data's shuffleBlockId with reduce ID set to 0 and add ".data"
 * as the filename postfix for data file, and ".index" as the filename postfix for index file.
 *
 */

// TODO: 这个文件非常重要，负责提供 block id 和物理文件（或者文件组）的对应关系，一定要看懂

// Note: Changes to the format in this file should be kept in sync with
// org.apache.spark.network.shuffle.ExternalShuffleBlockResolver#getSortBasedShuffleBlockData().
private[spark] class IndexShuffleBlockResolver(
    conf: SparkConf,
    _blockManager: BlockManager = null)
  extends ShuffleBlockResolver
  with Logging {

  private lazy val blockManager = Option(_blockManager).getOrElse(SparkEnv.get.blockManager)   // lazy 这点很好玩啊，用到了临时再算。另外这种 Option.getOrElse 用法可以省去很多判断代码，真心简洁

  private val transportConf = SparkTransportConf.fromSparkConf(conf, "shuffle")   // 为了通过 Netty 等通过网络传输 SparkConf，第二个参数是 module

  // 数据文件和索引（index）文件是分开放的。返回的是 Java 原生的 File 对象。根据 shuffleBlockId 和 mapId 就可以找到这个文件啦
  def getDataFile(shuffleId: Int, mapId: Int): File = {
    blockManager.diskBlockManager.getFile(ShuffleDataBlockId(shuffleId, mapId, NOOP_REDUCE_ID))  // 只是返回个文件名罢了："shuffle_" + shuffleId + "_" + mapId + "_" + reduceId + ".data" 这里的 reduceId 恒为零
  }

  private def getIndexFile(shuffleId: Int, mapId: Int): File = {
    blockManager.diskBlockManager.getFile(ShuffleIndexBlockId(shuffleId, mapId, NOOP_REDUCE_ID)) // 只是返回个文件名罢了："shuffle_" + shuffleId + "_" + mapId + "_" + reduceId + ".index"
  }

  /**
   * Remove data file and index file that contain the output data from one map. 根据 mapId 和 shuffleId 删除两个文件，好简单啊
   * */
  def removeDataByMap(shuffleId: Int, mapId: Int): Unit = {
    var file = getDataFile(shuffleId, mapId)  // 真好用啊，类型推断，不用显式声明类型了
    if (file.exists()) {
      if (!file.delete()) {
        logWarning(s"Error deleting data ${file.getPath()}")
      }
    }

    file = getIndexFile(shuffleId, mapId)
    if (file.exists()) {
      if (!file.delete()) {
        logWarning(s"Error deleting index ${file.getPath()}")
      }
    }
  }

  /**
   * Check whether the given index and data files match each other.
   * If so, return the partition lengths in the data file. Otherwise return null.
   */
  private def checkIndexAndDataFile(index: File, data: File, blocks: Int): Array[Long] = {
    // the index file should have `block + 1` longs as offset. 先从最简单的判断开始
    if (index.length() != (blocks + 1) * 8) {
      return null     // null 就是 index 和 data 不 match
    }
    val lengths = new Array[Long](blocks)   // 根据 block 数目建立 length 数组，里面是每个 block / partition 的长度

    // Read the lengths of blocks
    val in = try {
      new DataInputStream(new NioBufferedFileInputStream(index))  // Index 是 File 对象
    } catch {
      case e: IOException =>
        return null
    }

    try {
      // 将各个偏移量 offset 转为每个 block 的长度数据
      var offset = in.readLong()
      if (offset != 0L) {  // 第一个 block 的偏移量当然是 0，如果不是说明文件结构错了
        return null
      }

      var i = 0
      while (i < blocks) {
        val off = in.readLong()
        lengths(i) = off - offset   // 这么简单啊。就是一个一个 block 的偏移量数据依次存放，没什么复杂结构
        offset = off
        i += 1
      }
    } catch {
      case e: IOException =>
        return null
    } finally {
      in.close()
    }

    // the size of data file should match with index file 各种条件判断，任何一步出错都说明文件格式不对
    if (data.length() == lengths.sum) {
      lengths
    } else {
      null
    }
  }

  /**
   * Write an index file with the offsets of each block, plus a final offset at the end for the
   * end of the output file. This will be used by getBlockData to figure out where each block
   * begins and ends.
   *
   * It will commit the data and index file as an atomic operation, use the existing ones, or
   * replace them with new ones. 如果存在 index 文件且符合要求，就使用现有的，否则替换成最新的
   *
   * Note: the `lengths` will be updated to match the existing index file if use the existing ones.
   * */
  def writeIndexFileAndCommit(
      shuffleId: Int,
      mapId: Int,
      lengths: Array[Long],   // 每个 block 的长度信息
      dataTmp: File): Unit = {  // 注意传入的是 dataTmp 临时文件
    val indexFile = getIndexFile(shuffleId, mapId)   // 得到要写入的 Index File 对象
    val indexTmp = Utils.tempFileWith(indexFile)     // 在相同目录下创建一个临时文件  new File(indexFile.getAbsolutePath + "." + UUID.randomUUID())
    try {  // 注意这里写入的是 indexTmp，不是最终文件
      val out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(indexTmp)))   // Java 常见的嵌套流结构
      Utils.tryWithSafeFinally {  // 这个函数非常有用，对于写入文件时尽量用它，避免 out.write 抛出异常后 out.close 也发生异常从而掩盖了之前的异常信息。TODO: Scala 这种传入代码块的表示方法太棒了
        // We take in lengths of each block, need to convert it to offsets.
        var offset = 0L
        out.writeLong(offset)    // TODO: 瞬间明白了，这里面写的每个都相当于 reduceId（递增），因此文件名中的 reduceId 才恒为零，因为组合成一个文件了！！！
        for (length <- lengths) {
          offset += length
          out.writeLong(offset)
        }
      } {
        out.close()
      }

      val dataFile = getDataFile(shuffleId, mapId)   // 先有 dataFile，然后写入 indexFile，并校验两者是否一致
      // There is only one IndexShuffleBlockResolver per executor, this synchronization make sure
      // the following check and rename are atomic.
      // TODO: 注意这里需要 synchronized，并没有竞争，只是为了确保检查、更名操作的原子性，这样才好 commit
      synchronized {
        val existingLengths = checkIndexAndDataFile(indexFile, dataFile, lengths.length)
        if (existingLengths != null) {  // 如果先有的 dataFile 和 indexFile 信息一致，并返回了各个 segments 的长度信息，那么就没必要替换先有文件了
          // Another attempt for the same task has already written our map outputs successfully,
          // so just use the existing partition lengths and delete our temporary map outputs.
          System.arraycopy(existingLengths, 0, lengths, 0, lengths.length)  // 将 existingLengths 数组拷贝到 lengths 数组中。尽量使用系统现有的库而不是自己写 for 循环
          if (dataTmp != null && dataTmp.exists()) {
            dataTmp.delete()
          }
          indexTmp.delete()
        } else {  // 需要更新 index 和 data 文件
          // This is the first successful attempt in writing the map outputs for this task,
          // so override any existing index and data files with the ones we wrote.
          if (indexFile.exists()) {
            indexFile.delete()
          }
          if (dataFile.exists()) {
            dataFile.delete()
          }
          if (!indexTmp.renameTo(indexFile)) {
            throw new IOException("fail to rename file " + indexTmp + " to " + indexFile)
          }
          if (dataTmp != null && dataTmp.exists() && !dataTmp.renameTo(dataFile)) {
            throw new IOException("fail to rename file " + dataTmp + " to " + dataFile)
          }
        }
      }  // synchronized 结束，一气呵成
    } finally {
      if (indexTmp.exists() && !indexTmp.delete()) {
        logError(s"Failed to delete temporary index file at ${indexTmp.getAbsolutePath}")
      }
    }
  }

  // 这个文件是核心的
  override def getBlockData(blockId: ShuffleBlockId): ManagedBuffer = {   // 注意分清 blockId 和 shuffleId 等，blockId 本质上是个文件名字符串，包含了 shuffleId、mapId、reduceId 等维度
    // The block is actually going to be a range of a single map output file for this map, so
    // find out the consolidated file, then the offset within that from our index
    val indexFile = getIndexFile(blockId.shuffleId, blockId.mapId)   // 模式匹配时的参数外部也可以直接访问

    val in = new DataInputStream(new FileInputStream(indexFile))   // TODO: 为什么不需要 Buffer 了？
    try {
      ByteStreams.skipFully(in, blockId.reduceId * 8)    // 跳过 blockId.reduceId * Long 类型长度 8，这是 Google Guava 库提供的功能。跳到本次 Block 的数据区 TODO: reduceId 其实就是写入的一个个 offset 顺序排列
      val offset = in.readLong()      // 数据文件的开始位置
      val nextOffset = in.readLong()  // 数据文件的结束位置
      new FileSegmentManagedBuffer(   // 让 FileSegmentManagedBuffer 负责提取数据，提供以 nio 或者 Stream 的方式来读取数据
        transportConf,
        getDataFile(blockId.shuffleId, blockId.mapId),  // 提供文件名和 offset 区间，即可得到数据
        offset,
        nextOffset - offset)
    } finally {
      in.close()
    }
  }

  override def stop(): Unit = {}
}

private[spark] object IndexShuffleBlockResolver {
  // No-op reduce ID used in interactions with disk store.
  // The disk store currently expects puts to relate to a (map, reduce) pair, but in the sort
  // shuffle outputs for several reduces are glommed into a single file.
  val NOOP_REDUCE_ID = 0
}

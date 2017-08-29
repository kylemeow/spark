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

import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.storage.ShuffleBlockId

private[spark]
/**
 * Implementers of this trait understand how to retrieve block data for a logical shuffle block
 * identifier (i.e. map, reduce, and shuffle). Implementations may use files or file segments to
 * encapsulate shuffle data. This is used by the BlockStore to abstract over different shuffle
 * implementations when shuffle data is retrieved.
 */
trait ShuffleBlockResolver {
  type ShuffleId = Int

  /**
   * Retrieve the data for the specified block. If the data for that block is not available,
   * throws an unspecified exception.
   */
  def getBlockData(blockId: ShuffleBlockId): ManagedBuffer    // 可以根据 ShuffleBlockId 来得到 Block 数据，而不必考虑具体是来自文件还是文件段还是其他地方。blockId 就是包含了 shuffleId、mapId、reduceId，可以生成文件名

  def stop(): Unit
}

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

package org.apache.spark.shuffle.remote

import java.util.UUID

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkEnv
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.storage.{BlockId, TempShuffleBlockId}

object RemoteShuffleUtils {

  private val env = SparkEnv.get

  /**
   * Something like [[org.apache.spark.util.Utils.tempFileWith()]], instead returning Path
   */
  def tempPathWith(path: Path): Path = {
    new Path(path.getName + "." + UUID.randomUUID())
  }

  def getPath(blockId: BlockId): Path = {
    new Path(blockId.name)
  }

  /**
   * Something like [[org.apache.spark.storage.DiskBlockManager.createTempShuffleBlock()]], instead
   * returning Path
   */
  def createTempShuffleBlock(): (TempShuffleBlockId, Path) = {
    var blockId = new TempShuffleBlockId(UUID.randomUUID())
    val fs = getPath(blockId).getFileSystem(new Configuration)
    while (fs.exists(getPath(blockId))) {
      blockId = new TempShuffleBlockId(UUID.randomUUID())
    }
    (blockId, getPath(blockId))
  }

  def getRemoteWriter(
      blockId: BlockId,
      file: Path,
      serializerInstance: SerializerInstance,
      bufferSize: Int,
      writeMetrics: ShuffleWriteMetrics): RemoteBlockObjectWriter = {
    val syncWrites = env.blockManager.conf.getBoolean("spark.shuffle.sync", false)
    new RemoteBlockObjectWriter(file, env.serializerManager, serializerInstance, bufferSize,
      syncWrites, writeMetrics, blockId)
  }
}

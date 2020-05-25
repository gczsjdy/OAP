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

package org.apache.spark.shuffle.sort.remote

import java.util.Optional
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.shuffle.api.metadata.{MapOutputMetadata, ShuffleMetadata, ShuffleOutputTracker}

class RemoteHadoopShuffleOutputTracker extends ShuffleOutputTracker {

  private val shuffleStatuses =
    new ConcurrentHashMap[Int, RemoteHadoopShuffleMetadata]()

  override def registerShuffle(shuffleId: Int): Unit = {
    shuffleStatuses.put(shuffleId,
      new RemoteHadoopShuffleMetadata(new ConcurrentHashMap[Long, RemoteHadoopMapOutputMetadata]()))
  }

  /**
    * Called when the shuffle with the given id is unregistered because it will no longer
    * be used by Spark tasks.
    */
  override def unregisterShuffle(shuffleId: Int, blocking: Boolean): Unit = {
    RemoteShuffleBlockResolver.active.removeDataByShuffleId(shuffleId)
  }

  override def registerMapOutput(
    shuffleId: Int,
    mapId: Int,
    mapTaskAttemptId: Long,
    mapOutputMetadata: MapOutputMetadata): Unit = {
    val mapperToMetadata = shuffleStatuses.get(shuffleId)
    mapperToMetadata.put(
      mapTaskAttemptId, mapOutputMetadata.asInstanceOf[RemoteHadoopMapOutputMetadata])
  }

  override def removeMapOutput(shuffleId: Int, mapId: Int, mapTaskAttemptId: Long): Unit = {}

  override def getShuffleMetadata(shuffleId: Int): Optional[ShuffleMetadata] = {
    Optional.of(shuffleStatuses.get(shuffleId))
  }

  override def isMapOutputAvailableExternally(
    shuffleId: Int,
    mapId: Int,
    mapTaskAttemptId: Long): Boolean = true
}

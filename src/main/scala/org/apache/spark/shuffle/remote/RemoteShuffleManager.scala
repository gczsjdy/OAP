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

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle._
import org.apache.spark.shuffle.sort.SerializedShuffleHandle

/**
  * In remote shuffle, data is written to a remote Hadoop compatible file system instead of local
  * disks.
  */
private[spark] class RemoteShuffleManager(conf: SparkConf) extends ShuffleManager with Logging {

  if (!conf.getBoolean("spark.shuffle.spill", true)) {
    logWarning(
      "spark.shuffle.spill was set to false, but this configuration is ignored as of Spark 1.6+." +
          " Shuffle will continue to spill to disk when necessary.")
  }

  /**
    * A mapping from shuffle ids to the number of mappers producing output for those shuffles.
    */
  private[this] val numMapsForShuffle = new ConcurrentHashMap[Int, Int]()

  override val shuffleBlockResolver = new RemoteShuffleBlockResolver()

  /**
    * Obtains a [[ShuffleHandle]] to pass to tasks.
    */
  override def registerShuffle[K, V, C](
      shuffleId: Int,
      numMaps: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    if (RemoteShuffleManager.canUseSerializedShuffle(dependency)) {
      new SerializedShuffleHandle[K, V](
        shuffleId, numMaps, dependency.asInstanceOf[ShuffleDependency[K, V, V]])
    } else {
      // Otherwise, buffer map outputs in a deserialized form:
      new BaseShuffleHandle(shuffleId, numMaps, dependency)
    }
  }

  /**
    * Get a reader for a range of reduce partitions (startPartition to endPartition-1, inclusive).
    * Called on executors by reduce tasks.
    */
  override def getReader[K, C](
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext): ShuffleReader[K, C] = {
    new BlockStoreShuffleReader(
      handle.asInstanceOf[BaseShuffleHandle[K, _, C]], startPartition, endPartition, context)
  }

  /** Get a writer for a given partition. Called on executors by map tasks. */
  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Int,
      context: TaskContext): ShuffleWriter[K, V] = {
    numMapsForShuffle.putIfAbsent(
      handle.shuffleId, handle.asInstanceOf[BaseShuffleHandle[_, _, _]].numMaps)
    val env = SparkEnv.get
    handle match {
      case unsafeShuffleHandle: SerializedShuffleHandle[K @unchecked, V @unchecked] =>
        throw new NotImplementedError("Coming soon~")
      case other: BaseShuffleHandle[K @unchecked, V @unchecked, _] =>
        new RemoteShuffleWriter(shuffleBlockResolver, other, mapId, context)
    }
  }

  /** Remove a shuffle's metadata from the ShuffleManager. */
  override def unregisterShuffle(shuffleId: Int): Boolean = {
    Option(numMapsForShuffle.remove(shuffleId)).foreach { numMaps =>
      (0 until numMaps).foreach { mapId =>
        shuffleBlockResolver.removeDataByMap(shuffleId, mapId)
      }
    }
    true
  }

  /** Shut down this ShuffleManager. */
  override def stop(): Unit = {
    shuffleBlockResolver.stop()
  }
}


private[spark] object RemoteShuffleManager extends Logging {

  val useSimpleShuffleWriterThisTime = true

  /**
    * Helper method for determining whether a shuffle should use an optimized serialized shuffle
    * path or whether it should fall back to the original path that operates on deserialized objects.
    */
  def canUseSerializedShuffle(dependency: ShuffleDependency[_, _, _]): Boolean = {
    !useSimpleShuffleWriterThisTime
  }
}

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

import org.apache.hadoop.conf.Configuration
import org.apache.spark._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle._
import org.apache.spark.shuffle.sort._

/**
  * In remote shuffle, data is written to a remote Hadoop compatible file system instead of local
  * disks.
  */
private[spark] class RemoteShuffleManager(private val conf: SparkConf) extends ShuffleManager with
    Logging {

  RemoteShuffleManager.setActive(this)

  logWarning("******** Remote Shuffle Manager is used ********")

  if (!conf.getBoolean("spark.shuffle.spill", true)) {
    logWarning(
      "spark.shuffle.spill was set to false, but this configuration is ignored as of Spark 1.6+." +
          " Shuffle will continue to spill to disk when necessary.")
  }

  /**
    * A mapping from shuffle ids to the number of mappers producing output for those shuffles.
    */
  private[this] val numMapsForShuffle = new ConcurrentHashMap[Int, Int]()

  override val shuffleBlockResolver = new RemoteShuffleBlockResolver(conf)

  /**
    * Obtains a [[ShuffleHandle]] to pass to tasks.
    */
  override def registerShuffle[K, V, C](
      shuffleId: Int,
      numMaps: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    if (RemoteShuffleManager.shouldBypassMergeSort(conf, dependency)) {
      // If there are fewer than spark.shuffle.sort.bypassMergeThreshold partitions and we don't
      // need map-side aggregation, then write numPartitions files directly and just concatenate
      // them at the end. This avoids doing serialization and deserialization twice to merge
      // together the spilled files, which would happen with the normal code path. The downside is
      // having multiple files open at a time and thus more memory allocated to buffers.
      new BypassMergeSortShuffleHandle[K, V](
        shuffleId, numMaps, dependency.asInstanceOf[ShuffleDependency[K, V, V]])
    } else if (RemoteShuffleManager.canUseSerializedShuffle(dependency, conf)) {
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
    new RemoteShuffleReader(
      handle.asInstanceOf[BaseShuffleHandle[K, _, C]],
      shuffleBlockResolver,
      startPartition,
      endPartition,
      context)
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
        new RemoteUnsafeShuffleWriter(
          env.blockManager,
          shuffleBlockResolver,
          context.taskMemoryManager(),
          unsafeShuffleHandle,
          mapId, context, env.conf)
      case bypassMergeSortHandle: BypassMergeSortShuffleHandle[K @unchecked, V @unchecked] =>
        new RemoteBypassMergeSortShuffleWriter(
          env.blockManager,
          shuffleBlockResolver,
          bypassMergeSortHandle,
          mapId,
          context,
          env.conf)
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

  private var _active: RemoteShuffleManager = _

  def setActive(update: RemoteShuffleManager): Unit = _active = update

  val getHadoopConf = {
    require(_active != null, "Active RemoteShuffleManager unassigned! It's probably never newed")
    // Remote shuffling deals with the disaggregated computing and storage architecture, so that the
    // configuration of storage cluster cannot be locally loaded to executors. This is why we use
    // false here.
    // We may need better method to load default HDFS configuration of the remote storage cluster
    val hadoopConf = new Configuration(false)
    (new SparkHadoopUtil).appendS3AndSparkHadoopConfigurations(_active.conf, hadoopConf)
    hadoopConf
  }

  /**
    * Make the decision also referring to a configuration
    */
  def canUseSerializedShuffle(dependency: ShuffleDependency[_, _, _], conf: SparkConf): Boolean = {
    val optimizedShuffleEnabled = conf.get(RemoteShuffleConf.REMOTE_OPTIMIZED_SHUFFLE_ENABLED)
    optimizedShuffleEnabled && SortShuffleManager.canUseSerializedShuffle(dependency)
  }

  def shouldBypassMergeSort(conf: SparkConf, dep: ShuffleDependency[_, _, _]): Boolean = {
    val bypassMergeThreshold = conf.get(RemoteShuffleConf.REMOTE_BYPASS_MERGE_THRESHOLD)
    shouldBypassMergeSort(bypassMergeThreshold, dep) &&
      SortShuffleWriter.shouldBypassMergeSort(conf, dep)
  }

  private def shouldBypassMergeSort(remoteBypassThreshold: Int, dep: ShuffleDependency[_, _, _])
  : Boolean = {
    // HDFS poorly handles large number of small files, so in remote shuffle, we decide using
    // bypass-merge shuffle by compared numMappers * numReducers with the threshold
    dep.rdd.partitions.length * dep.partitioner.numPartitions < remoteBypassThreshold
  }
}

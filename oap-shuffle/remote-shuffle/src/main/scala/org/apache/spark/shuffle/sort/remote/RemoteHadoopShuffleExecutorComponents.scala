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

import java.{lang, util}
import java.util.Optional

import scala.collection.JavaConverters._

import org.apache.spark.{ShuffleDependency, SparkConf, SparkEnv, TaskContext}
import org.apache.spark.internal.config.{REDUCER_MAX_BLOCKS_IN_FLIGHT_PER_ADDRESS, REDUCER_MAX_REQS_IN_FLIGHT, REDUCER_MAX_SIZE_IN_FLIGHT}
import org.apache.spark.shuffle.api.{ShuffleExecutorComponents, ShuffleMapOutputWriter}
import org.apache.spark.shuffle.api.io.ShuffleBlockInputStream
import org.apache.spark.shuffle.api.metadata.{ShuffleBlockInfo, ShuffleMetadata, ShuffleUpdater}
import org.apache.spark.storage.{BlockId, ShuffleBlockId}

class RemoteHadoopShuffleExecutorComponents(conf: SparkConf) extends ShuffleExecutorComponents {

  private var resolver: RemoteShuffleBlockResolver = _

  override def initializeExecutor(
    appId: String, execId: String, extraConfigs: util.Map[String, String],
    updater: Optional[ShuffleUpdater]): Unit = {
    resolver = new RemoteShuffleBlockResolver(conf)
  }

  override def createMapOutputWriter(
    shuffleId: Int, mapTaskId: Long, numPartitions: Int): ShuffleMapOutputWriter = {
    new RemoteHadoopShuffleMapOutputWriter(shuffleId, mapTaskId, numPartitions, resolver, conf)
  }

  override def getPartitionReaders[K, V, C](
    blockInfos: lang.Iterable[ShuffleBlockInfo],
    dependency: ShuffleDependency[K, V, C],
    shuffleMetadata: Optional[ShuffleMetadata]): lang.Iterable[ShuffleBlockInputStream] = {
    val meta = shuffleMetadata.get().asInstanceOf[RemoteHadoopShuffleMetadata]
    new lang.Iterable[ShuffleBlockInputStream] {
      override def iterator(): util.Iterator[ShuffleBlockInputStream] =
        new RemoteShuffleBlockIterator(
          TaskContext.get(),
          resolver.remoteShuffleTransferService,
          resolver,
          blockInfos.asScala.groupBy(shuffleBlockInfo =>
            meta.get(shuffleBlockInfo.getMapTaskAttemptId).shuffleServerId)
            .map { case (blockManagerId, shuffleBlockInfos) =>
              (blockManagerId,
                shuffleBlockInfos.map(info =>
                  (ShuffleBlockId(
                    info.getShuffleId,
                    info.getMapTaskAttemptId,
                    info.getReduceId).asInstanceOf[BlockId],
                    info.getBlockLength,
                    info.getMapIndex)).toSeq)
            }.iterator,
          SparkEnv.get.serializerManager.wrapStream,
          // Note: we use getSizeAsMb when no suffix is provided for backwards compatibility
          conf.get(REDUCER_MAX_SIZE_IN_FLIGHT) * 1024 * 1024,
          conf.get(REDUCER_MAX_REQS_IN_FLIGHT),
          conf.get(REDUCER_MAX_BLOCKS_IN_FLIGHT_PER_ADDRESS),
          conf.getBoolean("spark.shuffle.detectCorrupt", true))
            .map(_._2).map(ShuffleBlockInputStream.of).asJava
    }
  }
}

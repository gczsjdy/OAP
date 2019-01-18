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

import java.io.{IOException, InputStream}

import javax.annotation.concurrent.GuardedBy
import org.apache.spark.internal.Logging
import org.apache.spark.network.shuffle._
import org.apache.spark.network.util.TransportConf
import org.apache.spark.storage.{BlockId, ShuffleBlockId}
import org.apache.spark.{SparkException, TaskContext}

import scala.collection.mutable

/**
  * An iterator that fetches multiple blocks. For local blocks, it fetches from the local block
  * manager. For remote blocks, it fetches them using the provided BlockTransferService.
  *
  * This creates an iterator of (BlockID, InputStream) tuples so the caller can handle blocks
  * in a pipelined fashion as they are received.
  *
  * The implementation throttles the remote fetches so they don't exceed maxBytesInFlight to avoid
  * using too much memory.
  *
  * @param context [[TaskContext]], used for metrics update
  * @param streamWrapper A function to wrap the returned input stream.
  */
private[spark]
final class RemoteShuffleBlockIterator(
    context: TaskContext,
    resolver: RemoteShuffleBlockResolver,
    shuffleId: Int,
    numMappers: Int,
    startPartition: Int,
    endPartition: Int,
    streamWrapper: (BlockId, InputStream) => InputStream)
    extends Iterator[(BlockId, InputStream)] with DownloadFileManager with Logging {

  private[this] val startTime = System.currentTimeMillis

  /**
    * The blocks that can't be decompressed successfully, it is used to guarantee that we retry
    * at most once for those corrupted blocks.
    */
  private[this] val corruptedBlocks = mutable.HashSet[BlockId]()

  private val insideIter = {
    for (mapId <- 0 until numMappers; reduceId <- startPartition until endPartition) yield {
      // Note by Chenzhao: Can be optimized by reading consecutive blocks
      val blockId = ShuffleBlockId(shuffleId, mapId, reduceId)
      val buf = resolver.getBlockData(blockId)

      val in = try {
        buf.createInputStream()
      } catch {
        // The exception could only be throwed by local shuffle block
        case e: IOException =>
          logError("Failed to create input stream from local block", e)
          buf.release()
          throwCurrentlySimpleMayChangeException(blockId, e)
      }
      val input = streamWrapper(blockId, in)
      (blockId, input)
    }
  }.toIterator

  /**
    * A set to store the files used for shuffling remote huge blocks. Files in this set will be
    * deleted when cleanup. This is a layer of defensiveness against disk file leaks.
    */
  @GuardedBy("this")
  private[this] val shuffleFilesSet = mutable.HashSet[DownloadFile]()

  /**
    */
  private[this] def cleanup() {
  }

  override def hasNext: Boolean = insideIter.hasNext

  /**
    * Fetches the next (BlockId, InputStream). If a task fails, the ManagedBuffers
    * underlying each InputStream will be freed by the cleanup() method registered with the
    * TaskCompletionListener. However, callers should close() these InputStreams
    * as soon as they are no longer needed, in order to release memory as early as possible.
    *
    * Throws a FetchFailedException if the next block could not be fetched.
    */
  override def next(): (BlockId, InputStream) = {
    if (!hasNext) {
      throw new NoSuchElementException
    }
    insideIter.next()
  }

  private def throwCurrentlySimpleMayChangeException(blockId: ShuffleBlockId, e: Throwable) = {
    blockId match {
      case ShuffleBlockId(shufId, mapId, reduceId) =>
        throw new SparkException(
          s"Hadoop FS read failed for shuffle: $shufId, map: $mapId, reduce: $reduceId")
    }
  }

  override def createTempFile(transportConf: TransportConf): DownloadFile = ???

  override def registerTempFileToClean(downloadFile: DownloadFile): Boolean = ???
}

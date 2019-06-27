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
import java.nio.ByteBuffer
import java.util.concurrent.LinkedBlockingQueue

import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, Queue}
import org.apache.spark.{SparkConf, SparkException, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.network.shuffle._
import org.apache.spark.network.util.TransportConf
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.storage.{BlockException, BlockId, BlockManagerId, ShuffleBlockId}
import org.apache.spark.util.Utils
import org.apache.spark.util.io.ChunkedByteBufferOutputStream

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
 * @param shuffleClient [[ShuffleClient]] for fetching remote blocks
 * @param blockManager [[BlockManager]] for reading local blocks
 * @param blocksByAddress list of blocks to fetch grouped by the [[BlockManagerId]].
 *                        For each block we also require the size (in bytes as a long field) in
 *                        order to throttle the memory usage. Note that zero-sized blocks are
 *                        already excluded, which happened in
 *                        [[MapOutputTracker.convertMapStatuses]].
 * @param streamWrapper A function to wrap the returned input stream.
 * @param maxBytesInFlight max size (in bytes) of remote blocks to fetch at any given point.
 * @param maxReqsInFlight max number of remote requests to fetch blocks at any given point.
 * @param maxBlocksInFlightPerAddress max number of shuffle blocks being fetched at any given point
 *                                    for a given remote host:port.
 * @param maxReqSizeShuffleToMem max size (in bytes) of a request that can be shuffled to memory.
 * @param detectCorrupt whether to detect any corruption in fetched blocks.
 */
private[spark]
final class RemoteShuffleBlockIterator(
    context: TaskContext,
    shuffleClient: ShuffleClient,
    resolver: RemoteShuffleBlockResolver,
    blocksByAddress: Iterator[(BlockManagerId, Seq[(BlockId, Long)])],
    streamWrapper: (BlockId, InputStream) => InputStream,
    maxBytesInFlight: Long,
    maxReqsInFlight: Int,
    maxBlocksInFlightPerAddress: Int,
    conf: SparkConf)
  extends Iterator[(BlockId, InputStream)] with Logging {

  import RemoteShuffleBlockIterator._

  private val indexCacheEnabled = resolver.indexCacheEnabled

  private val readMetrics = context.taskMetrics().createTempShuffleReadMetrics()

  /**
   * Total number of blocks to fetch. This should be equal to the total number of blocks
   * in [[blocksByAddress]] because we already filter out zero-sized blocks in [[blocksByAddress]].
   *
   * This should equal localBlocks.size + remoteBlocks.size.
   */
  private[this] var numBlocksToFetch = 0

  /**
   * The number of blocks processed by the caller. The iterator is exhausted when
   * [[numBlocksProcessed]] == [[numBlocksToFetch]].
   */
  private[this] var numBlocksProcessed = 0

  private[this] val startTime = System.currentTimeMillis

  /** Remote blocks to fetch, excluding zero-sized blocks. */
  private[this] val remoteBlocks = new HashSet[BlockId]()

  /**
   * A queue to hold our results. This turns the asynchronous model provided by
   * [[org.apache.spark.network.BlockTransferService]] into a synchronous model (iterator).
   */
  private[this] val results = new LinkedBlockingQueue[RemoteFetchResult]

  /**
   * Current [[RemoteFetchResult]] being processed. We track this so we can release the current buffer
   * in case of a runtime exception when processing the current buffer.
   */
  @volatile private[this] var currentResult: SuccessRemoteFetchResult = null

  /**
   * Queue of fetch requests to issue; we'll pull requests off this gradually to make sure that
   * the number of bytes in flight is limited to maxBytesInFlight.
   */
  private[this] val fetchRequests = new Queue[RemoteFetchRequest]

  /** Current bytes in flight from our requests */
  private[this] var bytesInFlight = 0L

  /** Current number of requests in flight */
  private[this] var reqsInFlight = 0

  /**
    * Whether the iterator is still active. If isZombie is true, the callback interface will no
    * longer place fetched blocks into [[results]].
    */
  @GuardedBy("this")
  private[this] var isZombie = false

  initialize()

  // Decrements the buffer reference count.
  // The currentResult is set to null to prevent releasing the buffer again on cleanup()
  private[remote] def releaseCurrentResultBuffer(): Unit = {
    // Release the current buffer if necessary
    if (currentResult != null) {
      currentResult.buf.release()
    }
    currentResult = null
  }

  /**
    * Mark the iterator as zombie, and release all buffers that haven't been deserialized yet.
    */
  private[this] def cleanup() {
    synchronized {
      isZombie = true
    }
    releaseCurrentResultBuffer()
    // Release buffers in the results queue
    val iter = results.iterator()
    while (iter.hasNext) {
      val result = iter.next()
      result match {
        case SuccessRemoteFetchResult(_, buf) =>
          buf.release()
        case _ =>
      }
    }
  }

  private[this] def sendRequest(req: RemoteFetchRequest) {
    reqsInFlight += 1

    // so we can look up the size of each blockID
    val blockIds = req.blocks.map(_._1)
    val address = req.address

    val blockFetchingListener = new BlockFetchingListener {
      override def onBlockFetchSuccess(blockId: String, buf: ManagedBuffer): Unit = {
        // Only add the buffer to results queue if the iterator is not zombie,
        // i.e. cleanup() has not been called yet.
        RemoteShuffleBlockIterator.this.synchronized {
          if (!isZombie) {
            // Increment the ref count because we need to pass this to a different thread.
            // This needs to be released after use.
            buf.retain()
            results.put(new SuccessRemoteFetchResult(BlockId(blockId), buf))
          }
        }
      }

      override def onBlockFetchFailure(blockId: String, e: Throwable): Unit = {
        logError(s"Failed to get block(s) ", e)
        results.put(new FailureRemoteFetchResult(BlockId(blockId), e))
      }
    }
    if (indexCacheEnabled) {
      shuffleClient.fetchBlocks(
        address.host, address.port, address.executorId, blockIds.map(_.toString()).toArray,
        blockFetchingListener, null)
    } else {
      fetchBlocks(blockIds.toArray, blockFetchingListener)
    }
  }

  private def fetchBlocks(
      blockIds: Array[BlockId],
      listener: BlockFetchingListener) = {
    for (blockId <- blockIds) {
      // Note by Chenzhao: Can be optimized by reading consecutive blocks
      try {
        val buf = resolver.getBlockData(blockId.asInstanceOf[ShuffleBlockId])
        listener.onBlockFetchSuccess(blockId.toString(), buf)
      } catch {
        case e: Exception => listener.onBlockFetchFailure(blockId.toString(), e)
      }
    }
  }

  // For remote shuffling, all blocks are remote, so this actually resembles RemoteFetchRequests
  private[this] def splitLocalRemoteBlocks(): ArrayBuffer[RemoteFetchRequest] = {

    // Split local and remote blocks. Remote blocks are further split into FetchRequests of size
    // at most maxBytesInFlight in order to limit the amount of data in flight.
    val remoteRequests = new ArrayBuffer[RemoteFetchRequest]

    if (indexCacheEnabled) {
      for ((address, blockInfos) <- blocksByAddress) {
        val iterator = blockInfos.iterator
        var curRequestSize = 0L
        var curBlocks = new ArrayBuffer[(BlockId, Long)]
        while (iterator.hasNext) {
          val (blockId, size) = iterator.next()
          if (size < 0) {
            throw new BlockException(blockId, "Negative block size " + size)
          } else if (size == 0) {
            throw new BlockException(blockId, "Zero-sized blocks should be excluded.")
          } else {
            curBlocks += ((blockId, size))
            remoteBlocks += blockId
            numBlocksToFetch += 1
            curRequestSize += size
          }
          // We only care about the amount of requests, but not the total content size of blocks, due
          // to during this fetch process we only get a range(offset and length) for each block. The
          // block content will not be transferred through netty, while it's read from a
          // globally-accessible Hadoop compatible file system
          if (curBlocks.size >= maxBlocksInFlightPerAddress) {
            // Add this FetchRequest
            remoteRequests += new RemoteFetchRequest(address, curBlocks)
            logDebug(s"Creating fetch request of $curRequestSize at $address "
                + s"with ${curBlocks.size} blocks")
            curBlocks = new ArrayBuffer[(BlockId, Long)]
            curRequestSize = 0
          }
        }
        // Add in the final request
        if (curBlocks.nonEmpty) {
          remoteRequests += new RemoteFetchRequest(address, curBlocks)
        }
      }
      logInfo(s"Getting $numBlocksToFetch non-empty blocks including ${remoteBlocks.size}" +
          s" remote blocks")
    } else {
      // Each request contains the ShuffleBlocks from the same mapper
      def cartesianProduct(blockInfo: (BlockManagerId, Seq[(BlockId, Long)])):
      Seq[(BlockManagerId, (BlockId, Long))] = {
        val (left, right) = blockInfo
        for (each <- right) yield (left, (each._1, each._2))
      }
      // Each request contains the ShuffleBlocks from the same mapper
      blocksByAddress.flatMap(cartesianProduct).toArray
          .groupBy(_._2._1.asInstanceOf[ShuffleBlockId].mapId).foreach {
        case (_, blockInfos) =>
          remoteRequests += new RemoteFetchRequest(blockInfos(0)._1, blockInfos.map(_._2))
          numBlocksToFetch += blockInfos.length
      }
    }
    remoteRequests
  }

  private[this] def initialize(): Unit = {
    // Add a task completion callback (called in both success case and failure case) to cleanup.
    context.addTaskCompletionListener[Unit](_ => cleanup())

    // Split local and remote blocks. Actually it assembles remote fetch requests due to all blocks
    // are remote under remote shuffle
    val remoteRequests = splitLocalRemoteBlocks()
    // Add the remote requests into our queue in a random order
    fetchRequests ++= Utils.randomize(remoteRequests)
    assert ((0 == reqsInFlight) == (0 == bytesInFlight),
      "expected reqsInFlight = 0 but found reqsInFlight = " + reqsInFlight +
      ", expected bytesInFlight = 0 but found bytesInFlight = " + bytesInFlight)

    // Send out initial requests for blocks, up to our maxBytesInFlight
    fetchUpToMaxBytes()

    val numFetches = remoteRequests.size - fetchRequests.size
    logInfo("Started " + numFetches + " remote fetches in" + Utils.getUsedTimeMs(startTime))

  }

  override def hasNext: Boolean = numBlocksProcessed < numBlocksToFetch

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

    numBlocksProcessed += 1

    var result: RemoteFetchResult = null
    var input: InputStream = null
    // Take the next fetched result and try to decompress it to detect data corruption,
    // then fetch it one more time if it's corrupt, throw FailureFetchResult if the second fetch
    // is also corrupt, so the previous stage could be retried.
    // For local shuffle block, throw FailureFetchResult for the first IOException.
    while (result == null) {
      val startFetchWait = System.currentTimeMillis()
      result = results.take()
      val stopFetchWait = System.currentTimeMillis()
      readMetrics.incFetchWaitTime(stopFetchWait - startFetchWait)

      result match {
        case r @ SuccessRemoteFetchResult(blockId, buf) =>
          val in = try {
            readMetrics.incRemoteBytesRead(buf.size())
            buf.createInputStream()
          } catch {
            case e: IOException =>
              // Actually here we know the buf is a HadoopFileSegmentManagedBuffer
              logError("Failed to create input stream from block backed by HDFS file segment", e)
              buf.release()
              throwDetailedException(blockId.asInstanceOf[ShuffleBlockId], e)
          }
          input = streamWrapper(blockId, in)

        case FailureRemoteFetchResult(blockId, e) =>
          throwDetailedException(blockId.asInstanceOf[ShuffleBlockId], e)
      }

      // Send fetch requests up to maxBytesInFlight
      fetchUpToMaxBytes()
    }

    currentResult = result.asInstanceOf[SuccessRemoteFetchResult]
    (currentResult.blockId, new RemoteBufferReleasingInputStream(input, this))
  }

  private def fetchUpToMaxBytes(): Unit = {
    // Process any regular fetch requests if possible.
    while (fetchRequests.nonEmpty) {
      val request = fetchRequests.dequeue()
      sendRequest(request)
    }
  }

  private def throwDetailedException(blockId: ShuffleBlockId, e: Throwable) = {
    blockId match {
      case ShuffleBlockId(shufId, mapId, reduceId) =>
        throw new SparkException(
          s"Hadoop FS read failed for shuffle: $shufId, map: $mapId, reduce: $reduceId")
    }
  }
}

/**
 * Helper class that ensures a ManagedBuffer is released upon InputStream.close()
 */
private class RemoteBufferReleasingInputStream(
    private val delegate: InputStream,
    private val iterator: RemoteShuffleBlockIterator)
  extends InputStream {
  private[this] var closed = false

  override def read(): Int = delegate.read()

  override def close(): Unit = {
    if (!closed) {
      delegate.close()
      iterator.releaseCurrentResultBuffer()
      closed = true
    }
  }

  override def available(): Int = delegate.available()

  override def mark(readlimit: Int): Unit = delegate.mark(readlimit)

  override def skip(n: Long): Long = delegate.skip(n)

  override def markSupported(): Boolean = delegate.markSupported()

  override def read(b: Array[Byte]): Int = delegate.read(b)

  override def read(b: Array[Byte], off: Int, len: Int): Int = delegate.read(b, off, len)

  override def reset(): Unit = delegate.reset()
}

private[remote]
object RemoteShuffleBlockIterator {

  /**
   * A request to fetch blocks from a remote BlockManager.
   * @param address remote BlockManager to fetch from.
   * @param blocks Sequence of tuple, where the first element is the block id,
   *               and the second element is the estimated size, used to calculate bytesInFlight.
   */
  case class RemoteFetchRequest(address: BlockManagerId, blocks: Seq[(BlockId, Long)])

  /**
   * Result of a fetch from a remote block.
   */
  private[remote] sealed trait RemoteFetchResult {
    val blockId: BlockId
  }

  /**
   * Result of a fetch from a remote block successfully.
   * @param blockId block id
   * @param buf `ManagedBuffer` for the content.
   */
  private[remote] case class SuccessRemoteFetchResult(
      blockId: BlockId,
      buf: ManagedBuffer) extends RemoteFetchResult {
    require(buf != null)
  }

  /**
   * Result of a fetch from a remote block unsuccessfully.
   * @param blockId block id
   * @param e the failure exception
   */
  private[remote] case class FailureRemoteFetchResult(
      blockId: BlockId,
      e: Throwable) extends RemoteFetchResult
}

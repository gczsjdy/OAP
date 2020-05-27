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

import java.io.{BufferedOutputStream, FileOutputStream, OutputStream}

import org.apache.hadoop.fs.{FSDataOutputStream, Path}

import org.apache.spark.SparkConf
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.shuffle.api.{ShuffleMapOutputWriter, ShufflePartitionWriter}
import org.apache.spark.shuffle.api.metadata.MapOutputCommitMessage

class RemoteHadoopShuffleMapOutputWriter(
    shuffleId: Int,
    mapId: Long,
    numPartitions: Int,
    resolver: RemoteShuffleBlockResolver,
    conf: SparkConf) extends ShuffleMapOutputWriter with Logging {

  private lazy val fs = resolver.fs

  private val partitionLengths: Array[Long] = new Array[Long](numPartitions)
  private val bufferSize = conf.get(config.SHUFFLE_UNSAFE_FILE_OUTPUT_BUFFER_SIZE).toInt * 1024

  private val tempFile: Path = resolver.createTempShuffleBlock()._2
  private var hadoopOutputStream: FSDataOutputStream = _
  private var bufferedOutputStream: OutputStream = _

  private def initStream(): Unit = {
    if (hadoopOutputStream == null) {
      hadoopOutputStream = fs.create(tempFile)
    }
    if (bufferedOutputStream == null) {
      bufferedOutputStream = new BufferedOutputStream(hadoopOutputStream, bufferSize)
    }
  }

  private def cleanUp(): Unit = {
    if (bufferedOutputStream != null) {
      bufferedOutputStream.close()
    }
    if (hadoopOutputStream != null) {
      hadoopOutputStream.close()
    }
  }

  override def getPartitionWriter(reducePartitionId: Int): ShufflePartitionWriter = {
    new RemoteHadoopShufflePartitionWriter(reducePartitionId)
  }

  override def commitAllPartitions: MapOutputCommitMessage = {
    cleanUp()
    resolver.writeIndexFileAndCommit(shuffleId, mapId, partitionLengths, tempFile)
    MapOutputCommitMessage.of(
      partitionLengths, new RemoteHadoopMapOutputMetadata(resolver.shuffleServerId))
  }

  override def abort(error: Throwable): Unit = {
    if (fs.exists(tempFile) && !fs.delete(tempFile, true)) {
      logWarning(s"Failed to delete temporary shuffle file at ${tempFile.toString}")
    }
  }

  class RemoteHadoopShufflePartitionWriter(reducePartitionId: Int) extends ShufflePartitionWriter {

    private var partStream: PartitionWriterStream = _

    override def openStream(): OutputStream = {
      if (partStream == null) {
        initStream()
        partStream = new PartitionWriterStream(reducePartitionId)
      }
      partStream
    }

    override def getNumBytesWritten: Long = partStream.getCount
  }

  class PartitionWriterStream(val partitionId: Int) extends OutputStream {
    private var count = 0
    private var isClosed = false

    def getCount: Int = count

    override def write(b: Int): Unit = {
      verifyNotClosed()
      bufferedOutputStream.write(b)
      count += 1
    }

    override def write(buf: Array[Byte], pos: Int, length: Int): Unit = {
      verifyNotClosed()
      bufferedOutputStream.write(buf, pos, length)
      count += length
    }

    override def close(): Unit = {
      isClosed = true
      partitionLengths(partitionId) = count
    }

    private def verifyNotClosed(): Unit = {
      if (isClosed) {
        throw new IllegalStateException("Attempting to write to a closed block output stream.")
      }
    }
  }
}

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

import java.io.{ByteArrayInputStream, InputStream, IOException}
import java.nio.ByteBuffer

import io.netty.buffer.{ByteBuf, Unpooled}
import org.apache.hadoop.fs.{FSDataInputStream, Path}

import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.protocol.{Encodable, Encoders}
import org.apache.spark.network.util.{JavaUtils, LimitedInputStream}

/**
  * Something like [[org.apache.spark.network.buffer.FileSegmentManagedBuffer]], instead we only
  * need createInputStream function, so we don't need a TransportConf field, which is intended to
  * be used in other functions
  */
private[spark] class HadoopFileSegmentManagedBuffer(
    val file: Path, val offset: Long, val length: Long)
    extends ManagedBuffer with Logging {

  import HadoopFileSegmentManagedBuffer.fs

  private lazy val byteBuffer: ByteBuffer = {
    var is: FSDataInputStream = null
    try {
      is = fs.open(file)
      is.seek(offset)
      val array = new Array[Byte](length.toInt)
      is.read(array)
      ByteBuffer.wrap(array)
    } finally {
      JavaUtils.closeQuietly(is)
    }
  }

  private lazy val nettyByteBuffer = Unpooled.wrappedBuffer(byteBuffer)

  override def size(): Long = length

  override def nioByteBuffer(): ByteBuffer = ???

  override def createInputStream(): InputStream = {

    if (length == 0) {
      new ByteArrayInputStream(new Array[Byte](0))
    } else {
      var is: FSDataInputStream = null
      // Note by Chenzhao: Why??
      var shouldClose = true

      try {
        is = fs.open(file)
        is.seek(offset)
        val r = new LimitedInputStream(is, length)
        shouldClose = false
        r
      } catch {
        case e: IOException =>
          var errorMessage = "Error in reading " + this
          if (is != null) {
            val size = fs.getFileStatus(file).getLen
            errorMessage = "Error in reading " + this + " (actual file length " + size + ")"
          }
          throw new IOException(errorMessage, e)
      } finally {
        if (shouldClose) JavaUtils.closeQuietly(is)
      }
    }

  }

  override def equals(obj: Any): Boolean = {
    if (! obj.isInstanceOf[HadoopFileSegmentManagedBuffer]) {
      false
    } else {
      val buffer = obj.asInstanceOf[HadoopFileSegmentManagedBuffer]
      this.file == buffer.file && this.offset == buffer.offset && this.length == buffer.length
    }
  }

  override def retain(): ManagedBuffer = this

  override def release(): ManagedBuffer = this

  override def convertToNetty(): AnyRef = nettyByteBuffer
}

private[remote] object HadoopFileSegmentManagedBuffer {
  private val fs = RemoteShuffleManager.getFileSystem
}

/**
  * This is an RPC message encapsulating HadoopFileSegmentManagedBuffers. Slightly different with
  * the OpenBlocks message, this doesn't transfer block stream between executors through netty, but
  * only returns file segment ranges(offsets and lengths). Due to in remote shuffle, there is a
  * globally-accessible remote storage, like HDFS.
  */
class MessageForHadoopManagedBuffers(
    val buffers: Array[(String, HadoopFileSegmentManagedBuffer)]) extends Encodable {

  override def encodedLength(): Int = {
    var sum = 0
    // the length of count: Int
    sum += 4
    for ((blockId, hadoopFileSegment) <- buffers) {
      sum += Encoders.Strings.encodedLength(blockId)
      sum += Encoders.Strings.encodedLength(hadoopFileSegment.file.toUri.toString)
      sum += 8
      sum += 8
    }
    sum
  }

  override def encode(buf: ByteBuf): Unit = {
    val count = buffers.length
    // To differentiate from other BlockTransferMessage
    buf.writeByte(MessageForHadoopManagedBuffers.MAGIC_CODE)
    buf.writeInt(count)
    for ((blockId, hadoopFileSegment) <- buffers) {
      Encoders.Strings.encode(buf, blockId)
      Encoders.Strings.encode(buf, hadoopFileSegment.file.toUri.toString)
      buf.writeLong(hadoopFileSegment.offset)
      buf.writeLong(hadoopFileSegment.length)
    }
  }

  // As opposed to fromByteBuffer
  def toByteBuffer: ByteBuf = {
    val buf = Unpooled.buffer(encodedLength)
    encode(buf)
    buf
  }
}

object MessageForHadoopManagedBuffers {

  // To differentiate from other BlockTransferMessage
  val MAGIC_CODE = -99

  // Decode
  def fromByteBuffer(buf: ByteBuf): MessageForHadoopManagedBuffers = {
    val magic = buf.readByte()
    assert(magic == MAGIC_CODE, "This is not a MessageForHadoopManagedBuffers! : (")
    val count = buf.readInt()
    val buffers = for (i <- 0 until count) yield {
      val blockId = Encoders.Strings.decode(buf)
      val path = new Path(Encoders.Strings.decode(buf))
      val offset = buf.readLong()
      val length = buf.readLong()
      (blockId, new HadoopFileSegmentManagedBuffer(path, offset, length))
    }
    new MessageForHadoopManagedBuffers(buffers.toArray)
  }
}

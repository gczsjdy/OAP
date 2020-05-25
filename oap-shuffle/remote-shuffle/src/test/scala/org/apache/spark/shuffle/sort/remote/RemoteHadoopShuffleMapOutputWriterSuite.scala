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

import java.io.ByteArrayOutputStream
import java.util.UUID

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.mockito.ArgumentMatchers.{any, anyInt, anyLong}
import org.mockito.Mockito.doAnswer
import org.mockito.Mockito.mock
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.network.util.LimitedInputStream
import org.apache.spark.storage.TempShuffleBlockId
import org.apache.spark.util.Utils

class RemoteHadoopShuffleMapOutputWriterSuite extends SparkFunSuite with BeforeAndAfterEach {

  private val conf = createDefaultConf(false)

  private val NUM_PARTITIONS = 10
  private val PARTITION_LEN = 100
  private val data: Array[Array[Byte]] = (0 until NUM_PARTITIONS).map {
    p => {
      if (p == 3) {
        Array.emptyByteArray
      } else {
        (0 to p * PARTITION_LEN).map(_ + p).map(_.toByte).toArray
      }
    }
  }.toArray
  private val partitionLengths = data.map(_.length)

  private var resultFile: Path = _
  private var resultFilePartitionSizes: Array[Long] = _
  private var mapOutputWriter: RemoteHadoopShuffleMapOutputWriter = _
  private var fs: FileSystem = _

  override def afterEach(): Unit = {
    try {
      fs.delete(resultFile, true)
    } finally {
      super.afterEach()
    }
  }

  def createDefaultConf(loadDefaults: Boolean = true): SparkConf = {
    new SparkConf(loadDefaults)
      .set("spark.shuffle.remote.storageMasterUri", "file://")
      .set("spark.shuffle.remote.filesRootDirectory", "/tmp")
  }

  override def beforeEach(): Unit = {
    val blockResolver = mock(classOf[RemoteShuffleBlockResolver])

    resultFile = new Path("file:///tmp/0_0.data")
    fs = resultFile.getFileSystem(new Configuration(false))

    when(blockResolver.getDataFile(anyInt, anyLong)).thenReturn(resultFile)
    when(blockResolver.createTempShuffleBlock).thenReturn(
      (TempShuffleBlockId(UUID.randomUUID()), new Path("file:///tmp/0_0.data.tmp")))
    when(blockResolver.fs).thenReturn(fs)
    when(blockResolver.writeIndexFileAndCommit(
      anyInt, anyLong, any(classOf[Array[Long]]), any(classOf[Path]))).thenAnswer(new Answer[Void] {
        def answer(invocationOnMock: InvocationOnMock): Void = {
          resultFilePartitionSizes = invocationOnMock.getArguments()(2).asInstanceOf[Array[Long]]
          val tmp: Path = invocationOnMock.getArguments()(3).asInstanceOf[Path]
          if (tmp != null) {
            fs.delete(resultFile, true)
            fs.rename(tmp, resultFile)
          }
          null
        }
      }
    )

    mapOutputWriter = new RemoteHadoopShuffleMapOutputWriter(
      0,
      0,
      NUM_PARTITIONS,
      blockResolver,
      conf)
  }

  private def readRecordsFromFile() = {
    var startOffset = 0L
    val result = new Array[Array[Byte]](NUM_PARTITIONS)
    (0 until NUM_PARTITIONS).foreach { p =>
      val partitionSize = data(p).length
      if (partitionSize > 0) {
        val in = fs.open(resultFile)
        in.seek(startOffset)
        val lin = new LimitedInputStream(in, partitionSize)
        val bytesOut = new ByteArrayOutputStream
        Utils.copyStream(lin, bytesOut, true, true)
        result(p) = bytesOut.toByteArray
      } else {
        result(p) = Array.emptyByteArray
      }
      startOffset += partitionSize
    }
    result
  }

  test("writing to an outputstream") {
    (0 until NUM_PARTITIONS).foreach { p =>
      val writer = mapOutputWriter.getPartitionWriter(p)
      val stream = writer.openStream()
      data(p).foreach { i => stream.write(i)}
      stream.close()
      intercept[IllegalStateException] {
        stream.write(p)
      }
      assert(writer.getNumBytesWritten() == data(p).length)
    }
    mapOutputWriter.commitAllPartitions()
    assert(resultFilePartitionSizes === partitionLengths)
    assert(fs.getFileStatus(resultFile).getLen === partitionLengths.sum)
    assert(data === readRecordsFromFile())
  }

}

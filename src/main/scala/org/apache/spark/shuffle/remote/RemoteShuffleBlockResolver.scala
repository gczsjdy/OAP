package org.apache.spark.shuffle.remote

import java.io._
import java.nio.ByteBuffer
import java.util.UUID

import com.google.common.io.ByteStreams
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.util.{JavaUtils, LimitedInputStream}
import org.apache.spark.shuffle.ShuffleBlockResolver
import org.apache.spark.storage.{ShuffleBlockId, TempLocalBlockId, TempShuffleBlockId}
import org.apache.spark.util.Utils

/**
  * Note by Chenzhao: optimization of index file cache
  *
  * Create and maintain the shuffle blocks' mapping between logic block and physical file location.
  * It also manages the resource cleaning and temporary files creation,
  * like a [[org.apache.spark.shuffle.IndexShuffleBlockResolver]] ++
  * [[org.apache.spark.storage.DiskBlockManager]]
  *
  */
class RemoteShuffleBlockResolver(conf: SparkConf) extends ShuffleBlockResolver with Logging {

  private val master = conf.get(RemoteShuffleConf.STORAGE_MASTER_URI)
  private val rootDir = conf.get(RemoteShuffleConf.SHUFFLE_FILES_ROOT_DIRECTORY)
  // 1. Use lazy evaluation due to at the time this class(and its fields) is initialized,
  // SparkEnv._conf is not yet set
  // 2. conf.getAppId may not always work, because during unit tests we may just new a Resolver
  // instead of getting one from the ShuffleManager referenced by SparkContext
  private lazy val applicationId =
    if (Utils.isTesting) s"test${UUID.randomUUID()}" else conf.getAppId
  private def dirPrefix = s"$master/$rootDir/$applicationId"

  private lazy val fs = new Path(dirPrefix).getFileSystem(RemoteShuffleManager.getHadoopConf)

/**
  * Something like [[org.apache.spark.storage.DiskBlockManager.getFile()]]
  */
  def getDataFile(shuffleId: Int, mapId: Int): Path = {
    new Path(s"${dirPrefix}/${shuffleId}_${mapId}.data")
  }

  def getIndexFile(shuffleId: Int, mapId: Int): Path = {
    new Path(s"${dirPrefix}/${shuffleId}_${mapId}.index")
  }

  /**
    * Write an index file with the offsets of each block, plus a final offset at the end for the
    * end of the output file. This will be used by getBlockData to figure out where each block
    * begins and ends.
    *
    * It will commit the data and index file as an atomic operation, use the existing ones, or
    * replace them with new ones.
    *
    * Note: the `lengths` will be updated to match the existing index file if use the existing ones.
    */
  def writeIndexFileAndCommit(
      shuffleId: Int,
      mapId: Int,
      lengths: Array[Long],
      dataTmp: Path): Unit = {

    val indexFile = getIndexFile(shuffleId, mapId)
    val indexTmp = RemoteShuffleUtils.tempPathWith(indexFile)
    try {
      val dataFile = getDataFile(shuffleId, mapId)
      // There is only one IndexShuffleBlockResolver per executor, this synchronization make sure
      // the following check and rename are atomic.
      synchronized {
        val existingLengths = checkIndexAndDataFile(indexFile, dataFile, lengths.length)
        if (existingLengths != null) {
          // Another attempt for the same task has already written our map outputs successfully,
          // so just use the existing partition lengths and delete our temporary map outputs.
          System.arraycopy(existingLengths, 0, lengths, 0, lengths.length)
          if (dataTmp != null && fs.exists(dataTmp)) {
            fs.delete(dataTmp, true)
          }
        } else {
          // This is the first successful attempt in writing the map outputs for this task,
          // so override any existing index and data files with the ones we wrote.
          val out = new DataOutputStream(new BufferedOutputStream(fs.create(indexTmp)))
          Utils.tryWithSafeFinally {
            // We take in lengths of each block, need to convert it to offsets.
            var offset = 0L
            out.writeLong(offset)
            for (length <- lengths) {
              offset += length
              out.writeLong(offset)
            }
          } {
            out.close()
          }

          if (fs.exists(indexFile)) {
            fs.delete(indexFile, true)
          }
          if (fs.exists(dataFile)) {
            fs.delete(dataFile, true)
          }
          if (!fs.rename(indexTmp, indexFile)) {
            throw new IOException("fail to rename file " + indexTmp + " to " + indexFile)
          }
          if (dataTmp != null && fs.exists(dataTmp) && !fs.rename(dataTmp, dataFile)) {
            throw new IOException("fail to rename file " + dataTmp + " to " + dataFile)
          }
        }
      }
    } finally {
      if (fs.exists(indexTmp) && !fs.delete(indexTmp, true)) {
        logError(s"Failed to delete temporary index file at ${indexTmp.getName}")
      }
    }
  }

  /**
    * Check whether the given index and data files match each other.
    * If so, return the partition lengths in the data file. Otherwise return null.
    */
  private def checkIndexAndDataFile(index: Path, data: Path, blocks: Int): Array[Long] = {
    val fs = index.getFileSystem(RemoteShuffleManager.getHadoopConf)

    // the index file should exist(of course) and have `block + 1` longs as offset.
    if (!fs.exists(index) || fs.getFileStatus(index).getLen != (blocks + 1) * 8L) {
      return null
    }
    val lengths = new Array[Long](blocks)
    // Read the lengths of blocks
    val in = try {
      // Note by Chenzhao: originally [[NioBufferedFileInputStream]] is used
      new DataInputStream(new BufferedInputStream(fs.open(index)))
    } catch {
      case e: IOException =>
        return null
    }
    try {
      // Convert the offsets into lengths of each block
      var offset = in.readLong()
      if (offset != 0L) {
        return null
      }
      var i = 0
      while (i < blocks) {
        val off = in.readLong()
        lengths(i) = off - offset
        offset = off
        i += 1
      }
    } catch {
      case e: IOException =>
        return null
    } finally {
      in.close()
    }

    // the size of data file should match with index file
    if (fs.exists(data) && fs.getFileStatus(data).getLen == lengths.sum) {
      lengths
    } else {
      null
    }
  }

  override def getBlockData(blockId: ShuffleBlockId): ManagedBuffer = {
    // The block is actually going to be a range of a single map output file for this map, so
    // find out the consolidated file, then the offset within that from our index
    val indexFile = getIndexFile(blockId.shuffleId, blockId.mapId)

    // SPARK-22982: if this FileInputStream's position is seeked forward by another piece of code
    // which is incorrectly using our file descriptor then this code will fetch the wrong offsets
    // (which may cause a reducer to be sent a different reducer's data). The explicit position
    // checks added here were a useful debugging aid during SPARK-22982 and may help prevent this
    // class of issue from re-occurring in the future which is why they are left here even though
    // SPARK-22982 is fixed.
    val fs = indexFile.getFileSystem(RemoteShuffleManager.getHadoopConf)
    val in = fs.open(indexFile)
    in.seek(blockId.reduceId * 8L)
    try {
      val offset = in.readLong()
      val nextOffset = in.readLong()
      val actualPosition = in.getPos()
      val expectedPosition = blockId.reduceId * 8L + 16
      if (actualPosition != expectedPosition) {
        throw new Exception(s"SPARK-22982: Incorrect channel position after index file reads: " +
            s"expected $expectedPosition but actual position was $actualPosition.")
      }
      new HadoopFileSegmentManagedBuffer(
        getDataFile(blockId.shuffleId, blockId.mapId),
        offset,
        nextOffset - offset)
    } finally {
      in.close()
    }
  }

  /**
    * Remove data file and index file that contain the output data from one map.
    */
  def removeDataByMap(shuffleId: Int, mapId: Int): Unit = {
    var file = getDataFile(shuffleId, mapId)
    val fs = file.getFileSystem(RemoteShuffleManager.getHadoopConf)
    if (fs.exists(file)) {
      if (!fs.delete(file, true)) {
        logWarning(s"Error deleting data ${file.toString}")
      }
    }

    file = getIndexFile(shuffleId, mapId)
    if (fs.exists(file)) {
      if (!fs.delete(file, true)) {
        logWarning(s"Error deleting index ${file.getName()}")
      }
    }
  }

  def createTempShuffleBlock(): (TempShuffleBlockId, Path) = {
    RemoteShuffleUtils.createTempShuffleBlock(dirPrefix)
  }

  def createTempLocalBlock(): (TempLocalBlockId, Path) = {
    RemoteShuffleUtils.createTempLocalBlock(dirPrefix)
  }

  override def stop(): Unit = {
    val dir = new Path(dirPrefix)
    val fs = dir.getFileSystem(RemoteShuffleManager.getHadoopConf)
    fs.delete(dir, true)
  }
}

/**
  * Something like [[org.apache.spark.network.buffer.FileSegmentManagedBuffer]], instead we only
  * need createInputStream function, so we don't need a TransportConf field, which is intended to
  * be used in other functions
  */
private[remote] class HadoopFileSegmentManagedBuffer(
    private val file: Path, private val offset: Long, private val length: Long)
    extends ManagedBuffer {

  override def size(): Long = length

  override def nioByteBuffer(): ByteBuffer = ???

  override def createInputStream(): InputStream = {

    if (length == 0) {
      new ByteArrayInputStream(new Array[Byte](0))
    } else {
      val fs = file.getFileSystem(RemoteShuffleManager.getHadoopConf)
      var is: InputStream = null
      var shouldClose = true

      try {
        is = fs.open(file)
        ByteStreams.skipFully(is, offset)
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
        if (shouldClose)
          JavaUtils.closeQuietly(is)
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

  override def convertToNetty(): AnyRef = ???
}

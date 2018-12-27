package org.apache.spark.shuffle.remote

import java.io._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.shuffle.ShuffleBlockResolver
import org.apache.spark.storage.ShuffleBlockId
import org.apache.spark.util.Utils

class RemoteShuffleBlockResolver extends ShuffleBlockResolver with Logging {

  private val prefix = RemoteShuffleUtils.getRemotePathPrefix

  def getDataFile(shuffleId: Int, mapId: Int): Path = {
    new Path(s"${prefix}_${shuffleId}_${mapId}")
  }

  def getIndexFile(shuffleId: Int, mapId: Int): Path = {
    new Path(s"${prefix}_${shuffleId}_${mapId}_index")
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
    val fs = dataTmp.getFileSystem(new Configuration)

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
    val fs = index.getFileSystem(new Configuration)

    // the index file should have `block + 1` longs as offset.
    if (fs.getFileStatus(index).getLen != (blocks + 1) * 8L) {
      return null
    }
    val lengths = new Array[Long](blocks)
    // Read the lengths of blocks
    val in = try {
      // By Chenzhao: originally [[NioBufferedFileInputStream]] is used
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
    if (fs.getFileStatus(data).getLen == lengths.sum) {
      lengths
    } else {
      null
    }
  }

  override def getBlockData(blockId: ShuffleBlockId): ManagedBuffer =
    throw new NotImplementedError("No need this for remote shuffle")

  /**
    * Remove data file and index file that contain the output data from one map.
    */
  def removeDataByMap(shuffleId: Int, mapId: Int): Unit = {
    var file = getDataFile(shuffleId, mapId)
    val fs = file.getFileSystem(new Configuration())
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

  override def stop(): Unit = ???
}

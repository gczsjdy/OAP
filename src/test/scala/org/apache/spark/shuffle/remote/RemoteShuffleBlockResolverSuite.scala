package org.apache.spark.shuffle.remote

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkFunSuite
import org.apache.spark.util.Utils
import org.scalatest.BeforeAndAfterEach

class RemoteShuffleBlockResolverSuite extends SparkFunSuite with BeforeAndAfterEach {

  test("Commit shuffle files multiple times") {

    val shuffleId = 1
    val mapId = 2

    val resolver = new RemoteShuffleBlockResolver
    val indexFile = resolver.getIndexFile(shuffleId, mapId)
    val dataFile = resolver.getDataFile(shuffleId, mapId)
    val dataTmp = RemoteShuffleUtils.tempPathWith(dataFile)
    val fs = dataFile.getFileSystem(new Configuration)

    val lengths = Array[Long](10, 0, 20)
    val out = fs.create(dataTmp)
    Utils.tryWithSafeFinally {
      out.write(new Array[Byte](30))
    } {
      out.close()
    }
    resolver.writeIndexFileAndCommit(shuffleId, mapId, lengths, dataTmp)

    assert(fs.exists(indexFile))
    assert(fs.getFileStatus(indexFile).getLen() === (lengths.length + 1) * 8)
    assert(fs.exists(dataFile))
    assert(fs.getFileStatus(dataFile).getLen() === 30)
    assert(!fs.exists(dataTmp))

    val lengths2 = new Array[Long](3)
    val dataTmp2 = RemoteShuffleUtils.tempPathWith(dataFile)
    val out2 = fs.create(dataTmp2)
    Utils.tryWithSafeFinally {
      out2.write(Array[Byte](1))
      out2.write(new Array[Byte](29))
    } {
      out2.close()
    }
    resolver.writeIndexFileAndCommit(shuffleId, mapId, lengths2, dataTmp2)

    assert(fs.getFileStatus(indexFile).getLen() === (lengths.length + 1) * 8)
    assert(lengths2.toSeq === lengths.toSeq)
    assert(fs.exists(dataFile))
    assert(fs.getFileStatus(dataFile).getLen() === 30)
    assert(!fs.exists(dataTmp2))

    // The dataFile should be the previous one
    val firstByte = new Array[Byte](1)
    val dataIn = fs.open(dataFile)
    Utils.tryWithSafeFinally {
      dataIn.read(firstByte)
    } {
      dataIn.close()
    }
    assert(firstByte(0) === 0)

    // The index file should not change
    val indexIn = fs.open(indexFile)
    Utils.tryWithSafeFinally {
      indexIn.readLong() // the first offset is always 0
      assert(indexIn.readLong() === 10, "The index file should not change")
    } {
      indexIn.close()
    }

    // remove data file
    fs.delete(dataFile, true)

    val lengths3 = Array[Long](7, 10, 15, 3)
    val dataTmp3 = RemoteShuffleUtils.tempPathWith(dataFile)
    val out3 = fs.create(dataTmp3)
    Utils.tryWithSafeFinally {
      out3.write(Array[Byte](2))
      out3.write(new Array[Byte](34))
    } {
      out3.close()
    }
    resolver.writeIndexFileAndCommit(shuffleId, mapId, lengths3, dataTmp3)
    assert(fs.getFileStatus(indexFile).getLen() === (lengths3.length + 1) * 8)
    assert(lengths3.toSeq != lengths.toSeq)
    assert(fs.exists(dataFile))
    assert(fs.getFileStatus(dataFile).getLen() === 35)
    assert(!fs.exists(dataTmp3))

    // The dataFile should be the new one, since we deleted the dataFile from the first attempt
    val dataIn2 = fs.open(dataFile)
    Utils.tryWithSafeFinally {
      dataIn2.read(firstByte)
    } {
      dataIn2.close()
    }
    assert(firstByte(0) === 2)

    // The index file should be updated, since we deleted the dataFile from the first attempt
    val indexIn2 = fs.open(indexFile)
    Utils.tryWithSafeFinally {
      indexIn2.readLong() // the first offset is always 0
      assert(indexIn2.readLong() === 7, "The index file should be updated")
    } {
      indexIn2.close()
    }
  }

}

package org.apache.spark.shuffle.remote

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.shuffle.ShuffleBlockResolver
import org.apache.spark.storage.ShuffleBlockId

class RemoteShuffleBlockResolver extends ShuffleBlockResolver with Logging {

  val prefix = s"hdfs:///shuffle/${SparkContext.getActive.get.applicationId}"

  def getDataFile(shuffleId: Int, mapId: Int): Path = {
    new Path(s"${prefix}_${shuffleId}_${mapId}")
  }

  override def getBlockData(blockId: ShuffleBlockId): ManagedBuffer = ???

  override def stop(): Unit = ???
}

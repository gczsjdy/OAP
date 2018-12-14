package org.apache.spark.shuffle.remote

import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.shuffle.ShuffleBlockResolver
import org.apache.spark.storage.ShuffleBlockId

class RemoteShuffleBlockResolver extends ShuffleBlockResolver with Logging {
  override def getBlockData(blockId: ShuffleBlockId): ManagedBuffer = ???

  override def stop(): Unit = ???
}

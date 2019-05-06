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

package org.apache.spark.util.collection

import org.apache.hadoop.fs.Path
import org.apache.spark._
import org.apache.spark.memory.MemoryTestingUtils
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer, SerializerInstance, SerializerManager}
import org.apache.spark.shuffle.IndexShuffleBlockResolver
import org.apache.spark.shuffle.remote.{RemoteAggregator, RemoteShuffleBlockResolver, RemoteShuffleUtils}
import org.apache.spark.storage.ShuffleBlockId

class RemoteSorterSuite extends SparkFunSuite with LocalSparkContext {

  var sorter: RemoteSorter[Int, Int, Int] = _

  testWithMultipleSer("no sorting or partial aggregation with spilling") { (conf: SparkConf) =>
    basicSorterTest(conf, withPartialAgg = false, withOrdering = false, withSpilling = true)
  }

  testWithMultipleSer("basic sorter write") { (conf: SparkConf) =>
    basicSorterWrite(conf, withPartialAgg = false, withOrdering = false, withSpilling = true)
  }

  /* ============================= *
   |  Helper test utility methods  |
   * ============================= */

  private def createSparkConf(loadDefaults: Boolean, kryo: Boolean): SparkConf = {
    val conf = createDefaultConf(loadDefaults)
    if (kryo) {
      conf.set("spark.serializer", classOf[KryoSerializer].getName)
    } else {
      // Make the Java serializer write a reset instruction (TC_RESET) after each object to test
      // for a bug we had with bytes written past the last object in a batch (SPARK-2792)
      conf.set("spark.serializer.objectStreamReset", "1")
        .set("spark.serializer", classOf[JavaSerializer].getName)
    }
    conf.set("spark.shuffle.sort.bypassMergeThreshold", "0")
    // Ensure that we actually have multiple batches per spill file
      .set("spark.shuffle.spill.batchSize", "10")
      .set("spark.shuffle.spill.initialMemoryThreshold", "512")
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.remote.RemoteShuffleManager")
  }

  /**
    * Run a test multiple times, each time with a different serializer.
    */
  private def testWithMultipleSer(
      name: String,
      loadDefaults: Boolean = false)(body: (SparkConf => Unit)): Unit = {
    test(name + " with kryo ser") {
      body(createSparkConf(loadDefaults, kryo = true))
    }
    test(name + " with java ser") {
      body(createSparkConf(loadDefaults, kryo = false))
    }
  }

  /* =========================================== *
   |  Helper methods that contain the test body  |
   * =========================================== */
  private def basicSorterTest(
      conf: SparkConf,
      withPartialAgg: Boolean,
      withOrdering: Boolean,
      withSpilling: Boolean) {
    val size = 1000
    if (withSpilling) {
      conf.set("spark.shuffle.spill.numElementsForceSpillThreshold", (size / 2).toString)
    }
    sc = new SparkContext("local", "test", conf)
    val shuffleManager = SparkEnv.get.shuffleManager
    val resolver = shuffleManager.shuffleBlockResolver.asInstanceOf[RemoteShuffleBlockResolver]
    val agg =
      if (withPartialAgg) {
        Some(new RemoteAggregator(
          new Aggregator[Int, Int, Int](i => i, (i, j) => i + j, (i, j) => i + j), resolver))
      } else {
        None
      }
    val ord = if (withOrdering) Some(implicitly[Ordering[Int]]) else None
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    sorter = new RemoteSorter[Int, Int, Int](
        context, resolver, agg, Some(new HashPartitioner(3)), ord)
    sorter.insertAll((0 until size).iterator.map { i => (i / 4, i) })
    if (withSpilling) {
      assert(sorter.numSpills > 0, "sorter did not spill")
    } else {
      assert(sorter.numSpills === 0, "sorter spilled")
    }
    val results = sorter.partitionedIterator.map { case (p, vs) => (p, vs.toSet) }.toSet
    val expected = (0 until 3).map { p =>
      var v = (0 until size).map { i => (i / 4, i) }.filter { case (k, _) => k % 3 == p }.toSet
      if (withPartialAgg) {
        v = v.groupBy(_._1).mapValues { s => s.map(_._2).sum }.toSet
      }
      (p, v.toSet)
    }.toSet
    assert(results === expected)
    sorter.stop()
  }

  private class SimpleRemoteBlockObjectReader[K, V](
      serializerManager: SerializerManager, serializerInstance: SerializerInstance) {

    def read(mapperInfo: ShuffleBlockId,
        startPartition: Int,
        endPartition: Int,
        resolver: RemoteShuffleBlockResolver,
        file: Path)
      : Iterator[Product2[K, V]] = {
      val fs = resolver.fs
      val inputStream = fs.open(file)
      (startPartition until endPartition).flatMap { i =>
        val blockId = ShuffleBlockId(mapperInfo.shuffleId, mapperInfo.mapId, i)
        val buf = resolver.getBlockData(blockId)

        val rawStream = buf.createInputStream()
        serializerInstance.deserializeStream(
          serializerManager.wrapStream(blockId, rawStream))
            .asKeyValueIterator.asInstanceOf[Iterator[Product2[K, V]]]
      }.toIterator
    }
  }

  private def basicSorterWrite(
      conf: SparkConf,
      withPartialAgg: Boolean,
      withOrdering: Boolean,
      withSpilling: Boolean) {
    val size = 1000
    if (withSpilling) {
      conf.set("spark.shuffle.spill.numElementsForceSpillThreshold", (size / 2).toString)
    }
    sc = new SparkContext("local", "test", conf)
    val shuffleManager = SparkEnv.get.shuffleManager
    val resolver = shuffleManager.shuffleBlockResolver.asInstanceOf[RemoteShuffleBlockResolver]
    val agg =
      if (withPartialAgg) {
        Some(new RemoteAggregator(
          new Aggregator[Int, Int, Int](i => i, (i, j) => i + j, (i, j) => i + j), resolver))
      } else {
        None
      }
    val ord = if (withOrdering) Some(implicitly[Ordering[Int]]) else None
    val context = MemoryTestingUtils.fakeTaskContext(sc.env)
    sorter = new RemoteSorter[Int, Int, Int](
      context, resolver, agg, Some(new HashPartitioner(3)), ord)
    sorter.insertAll((0 until size).iterator.map { i => (i / 4, i) })
    if (withSpilling) {
      assert(sorter.numSpills > 0, "sorter did not spill")
    } else {
      assert(sorter.numSpills === 0, "sorter spilled")
    }

    val (shuffleId, mapId) = (66, 666)
    val testShuffleBlockId = ShuffleBlockId(
      shuffleId, mapId, IndexShuffleBlockResolver.NOOP_REDUCE_ID)
    val path = resolver.getDataFile(shuffleId, mapId)
    val tmp = RemoteShuffleUtils.tempPathWith(path)
    val lengths = sorter.writePartitionedFile(testShuffleBlockId, tmp)
    resolver.writeIndexFileAndCommit(shuffleId, mapId, lengths, tmp)

    val results =
      new SimpleRemoteBlockObjectReader[Int, Int](
        sc.env.serializerManager, sc.env.serializer.newInstance()).read(
        testShuffleBlockId, 0, lengths.length, resolver, path).toSet
    val expected = (0 until size).map { i => (i / 4, i)}.toSet

    assert(results === expected)

  }

  override def afterEach(): Unit = {
    super.afterEach()
    if (sorter != null) {
      sorter.stop()
    }
  }

}

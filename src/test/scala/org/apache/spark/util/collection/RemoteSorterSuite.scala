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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark._
import org.apache.spark.memory.MemoryTestingUtils
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer, SerializerInstance, SerializerManager}
import org.apache.spark.shuffle.remote.{RemoteAggregator, RemoteShuffleBlockResolver}
import org.apache.spark.storage.TestBlockId

class RemoteSorterSuite extends SparkFunSuite with LocalSparkContext {

  var sorter: RemoteSorter[Int, Int, Int] = _
  var resolver: RemoteShuffleBlockResolver = _

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
    val conf = new SparkConf(loadDefaults)
    if (kryo) {
      conf.set("spark.serializer", classOf[KryoSerializer].getName)
    } else {
      // Make the Java serializer write a reset instruction (TC_RESET) after each object to test
      // for a bug we had with bytes written past the last object in a batch (SPARK-2792)
      conf.set("spark.serializer.objectStreamReset", "1")
      conf.set("spark.serializer", classOf[JavaSerializer].getName)
    }
    conf.set("spark.shuffle.sort.bypassMergeThreshold", "0")
    // Ensure that we actually have multiple batches per spill file
    conf.set("spark.shuffle.spill.batchSize", "10")
    conf.set("spark.shuffle.spill.initialMemoryThreshold", "512")
    conf
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
    /**
      * Note by Chenzhao: Need to revisit this, lack of append argument in HDFS API induces writing
      * a header every time for Java serializer. The current way of commitAndGet may need to be
      * modified
      */
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
    resolver = new RemoteShuffleBlockResolver(conf)
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
    def read(blockId: TestBlockId, file: Path): Iterator[Product2[K, V]] = {
      val fs = file.getFileSystem(new Configuration)
      val inputStream = fs.open(file)
      serializerInstance.deserializeStream(
        serializerManager.wrapStream(blockId, inputStream))
          .asKeyValueIterator.asInstanceOf[Iterator[Product2[K, V]]]
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
    resolver = new RemoteShuffleBlockResolver(conf)
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

    val testBlockId = TestBlockId("hi")
    val path = resolver.createTempShuffleBlock()._2
    sorter.writePartitionedFile(testBlockId, path)
    val results =
      new SimpleRemoteBlockObjectReader[Int, Int](
        sc.env.serializerManager, sc.env.serializer.newInstance()).read(testBlockId, path).toSet
    val expected = (0 until size).map { i => (i / 4, i)}.toSet

    assert(results === expected)

  }

  override def afterEach(): Unit = {
    super.afterEach()
    if (resolver != null) {
      resolver.stop()
    }
    if (sorter != null) {
      sorter.stop()
    }
  }

  private def cleanFiles(paths: Path*): Unit = {
    paths.foreach { path =>
      val fs = path.getFileSystem(new Configuration)
      fs.delete(path, true)
    }
  }
}

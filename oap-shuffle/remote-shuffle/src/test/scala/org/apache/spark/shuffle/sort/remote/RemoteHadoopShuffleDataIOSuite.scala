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

import org.apache.spark.{HashPartitioner, LocalSparkContext, ShuffleDependency, SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.internal.config
import org.apache.spark.util.Utils

class RemoteHadoopShuffleDataIOSuite extends SparkFunSuite with LocalSparkContext {

  testWithMultiplePath("repartition")(repartition(100, 10, 20))
  testWithMultiplePath("re-large-partition")(repartition(1000000, 3, 2))

  testWithMultiplePath(
    "repartition with some map output empty")(repartitionWithEmptyMapOutput)

  testWithMultiplePath("sort")(sort(500, 13, true))
  testWithMultiplePath("sort large partition")(sort(500000, 2))

  test("Remote shuffle and external shuffle service cannot be enabled at the same time") {
    intercept[Exception] {
      sc = new SparkContext(
        "local",
        "test",
        new SparkConf(true)
          .set("spark.shuffle.manager", "org.apache.spark.shuffle.remote.RemoteShuffleManager")
          .set("spark.shuffle.service.enabled", "true"))
    }
  }

  // Optimized shuffle writer & non-optimized shuffle writer
  private def testWithMultiplePath(name: String, loadDefaults: Boolean = true)
    (body: (SparkConf => Unit)): Unit = {
    test(name + " with general shuffle path") {
      body(createSparkConf(loadDefaults, bypassMergeSort = false, unsafeOptimized = false))
    }
    test(name + " with optimized shuffle path") {
      body(createSparkConf(loadDefaults, bypassMergeSort = false, unsafeOptimized = true))
    }
    test(name + " with bypass-merge-sort shuffle path") {
      body(createSparkConf(loadDefaults, bypassMergeSort = true, unsafeOptimized = false))
    }
    test(name + " with bypass-merge-sort shuffle path + index cache") {
      body(createSparkConf(loadDefaults,
        bypassMergeSort = true, unsafeOptimized = false, indexCache = true))
    }
    test(name + " with optimized shuffle path + index cache") {
      body(createSparkConf(loadDefaults,
        bypassMergeSort = false, unsafeOptimized = true, indexCache = true))
    }
    test(name + " with whatever shuffle write path + constraining maxBlocksPerAdress") {
      body(createSparkConf(loadDefaults, indexCache = false, setMaxBlocksPerAdress = true))
    }
    test(name + " with whatever shuffle write path + index cache + constraining maxBlocksPerAdress")
    {
      body(createSparkConf(loadDefaults, indexCache = true, setMaxBlocksPerAdress = true))
    }
    val default = RemoteShuffleConf.DATA_FETCH_EAGER_REQUIREMENT.defaultValue.get
    val testWith = (true ^ default)
    test(name + s" with eager requirement = ${testWith}")
    {
      body(createSparkConf(loadDefaults, indexCache = true)
        .set(RemoteShuffleConf.DATA_FETCH_EAGER_REQUIREMENT.key, testWith.toString))
    }
  }

  private def repartition(
    dataSize: Int, preShuffleNumPartitions: Int, postShuffleNumPartitions: Int)
    (conf: SparkConf): Unit = {
    sc = new SparkContext("local", "test_repartition", conf)
    val data = 0 until dataSize
    val rdd = sc.parallelize(data, preShuffleNumPartitions)
    val newRdd = rdd.repartition(postShuffleNumPartitions)
    assert(newRdd.collect().sorted === data)
  }

  private def repartitionWithEmptyMapOutput(conf: SparkConf): Unit = {
    sc = new SparkContext("local", "test_repartition_empty", conf)
    val data = 0 until 20
    val rdd = sc.parallelize(data, 30)
    val newRdd = rdd.repartition(40)
    assert(newRdd.collect().sorted === data)
  }

  private def sort(
    dataSize: Int, numPartitions: Int, differentMapSidePartitionLength: Boolean = false)
    (conf: SparkConf): Unit = {
    sc = new SparkContext("local", "sort", conf)
    val data = if (differentMapSidePartitionLength) {
      List.fill(dataSize/2)(0) ++ (dataSize / 2 until dataSize)
    } else {
      0 until dataSize
    }
    val rdd = sc.parallelize(Utils.randomize(data), numPartitions)

    val newRdd = rdd.sortBy((x: Int) => x.toLong)
    assert(newRdd.collect() === data)
  }

  private def createDefaultConf(loadDefaults: Boolean = true): SparkConf = {
    new SparkConf(loadDefaults)
      // Unit tests should not rely on external systems, using local file system as storage
      .set(RemoteShuffleConf.STORAGE_MASTER_URI, "file://")
      .set(RemoteShuffleConf.SHUFFLE_FILES_ROOT_DIRECTORY, "/tmp/remote_shuffle_io_testing")
      .set(config.SHUFFLE_IO_PLUGIN_CLASS, classOf[RemoteHadoopShuffleDataIO].getName)
  }

  private def createSparkConf(
    loadDefaults: Boolean, bypassMergeSort: Boolean = false, unsafeOptimized: Boolean = true,
    indexCache: Boolean = false, setMaxBlocksPerAdress: Boolean = false): SparkConf = {
    val smallThreshold = 1
    val largeThreshold = 50
    val conf = createDefaultConf(loadDefaults)
      .set("spark.shuffle.optimizedPathEnabled", unsafeOptimized.toString)
      // Use a strict threshold as default so that Bypass-Merge-Sort shuffle writer won't be used
      .set("spark.shuffle.sort.bypassMergeThreshold", smallThreshold.toString)
    if (bypassMergeSort) {
      // Use a loose threshold
      conf.set("spark.shuffle.sort.bypassMergeThreshold", largeThreshold.toString)
    }
    if (indexCache) {
      conf.set("spark.shuffle.remote.index.cache.size", "3m")
    }
    if (setMaxBlocksPerAdress) {
      conf.set(config.REDUCER_MAX_BLOCKS_IN_FLIGHT_PER_ADDRESS.key, "1")
    }
    conf
  }
}

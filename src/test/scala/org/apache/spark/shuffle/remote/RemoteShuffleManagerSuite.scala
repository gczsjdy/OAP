package org.apache.spark.shuffle.remote

import org.apache.spark._

class RemoteShuffleManagerSuite extends SparkFunSuite with LocalSparkContext {

  testWithMultiplePath("repartition")(repartition)

  testWithMultiplePath(
    "repartition with some map output empty")(repartitionWithEmptyMapOutput)

  testWithMultiplePath("sort")(sort)

  test("decide using bypass-merge-sort shuffle writer or not") {
    sc = new SparkContext("local", "test", new SparkConf(true))
    val partitioner = new HashPartitioner(100)
    val rdd = sc.parallelize((1 to 10).map(x => (x, x + 1)), 10)
    val dependency = new ShuffleDependency[Int, Int, Int](rdd, partitioner)
    assert(RemoteShuffleManager.shouldBypassMergeSort(new SparkConf(true), dependency)
        == false)
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
  }

  private def repartition(conf: SparkConf): Unit = {
    sc = new SparkContext("local", "test_repartition", conf)
    val data = 1 until 100
    val rdd = sc.parallelize(data, 10)
    val newRdd = rdd.repartition(20)
    assert(newRdd.collect().sorted === data)
  }

  private def repartitionWithEmptyMapOutput(conf: SparkConf): Unit = {
    sc = new SparkContext("local", "test_repartition_empty", conf)
    val data = 1 until 20
    val rdd = sc.parallelize(data, 30)
    val newRdd = rdd.repartition(40)
    assert(newRdd.collect().sorted === data)
  }

  private def sort(conf: SparkConf): Unit = {
    sc = new SparkContext("local", "sort", conf)
    val data = 1 until 500
    val rdd = sc.parallelize(data, 13)
    val newRdd = rdd.sortBy((x: Int) => x.toLong)
    assert(newRdd.collect() === data)
  }

  private def createSparkConf(
      loadDefaults: Boolean, bypassMergeSort: Boolean, unsafeOptimized: Boolean = true): SparkConf
    = {
    val smallThreshold = 1
    val largeThreshold = 50
    val conf = new SparkConf(loadDefaults)
      .set("spark.shuffle.optimizedPathEnabled", unsafeOptimized.toString)
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.remote.RemoteShuffleManager")
      // Use a strict threshold as default so that Bypass-Merge-Sort shuffle writer won't be used
      .set("spark.shuffle.sort.bypassMergeThreshold", smallThreshold.toString)
    if (bypassMergeSort) {
      // Use a loose threshold
      conf.set("spark.shuffle.sort.bypassMergeThreshold", largeThreshold.toString)
    }
    conf
  }

}

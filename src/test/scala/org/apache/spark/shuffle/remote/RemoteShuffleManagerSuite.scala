package org.apache.spark.shuffle.remote

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkFunSuite}

class RemoteShuffleManagerSuite extends SparkFunSuite with LocalSparkContext {

  testWithMultiplePath("repartition")(repartition)

  testWithMultiplePath(
    "repartition with some map output empty")(repartitionWithEmptyMapOutput)

  testWithMultiplePath("sort")(sort)

  // Optimized shuffle writer & non-optimized shuffle writer
  private def testWithMultiplePath(name: String, loadDefaults: Boolean = true)
      (body: (SparkConf => Unit)): Unit = {
    test(name + " with general shuffle path") {
      body(createSparkConf(loadDefaults, optimized = false))
    }
    test(name + " with optimized shuffle path") {
      body(createSparkConf(loadDefaults, optimized = true))
    }
  }

  private def repartition(conf: SparkConf): Unit = {
    sc = new SparkContext("local", "test_repartition", conf)
    val data = 1 until 20
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

  private def createSparkConf(loadDefaults: Boolean, optimized: Boolean): SparkConf = {
    new SparkConf(loadDefaults)
      .set("spark.shuffle.optimizedPathEnabled", optimized.toString)
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.remote.RemoteShuffleManager")
  }

  override def afterEach(): Unit = {
    super.afterEach()
    val dir = new Path(RemoteShuffleUtils.directoryPrefix)
    val fs = dir.getFileSystem(new Configuration)
    fs.delete(dir, true)
  }
}

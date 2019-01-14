package org.apache.spark.shuffle.remote

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkFunSuite}

class RemoteShuffleManagerSuite extends SparkFunSuite with LocalSparkContext {

  test("repartition") {
    val conf = new SparkConf(true)
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.remote.RemoteShuffleManager")
    sc = new SparkContext("local", "test", conf)
    val data = 1 until 20
    val rdd = sc.parallelize(data, 10)
    val newRdd = rdd.repartition(20)
    assert(newRdd.collect().sorted === data)
  }

  test("sort") {
    val conf = new SparkConf(true)
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.remote.RemoteShuffleManager")
    sc = new SparkContext("local", "test", conf)
    val data = 1 until 500
    val rdd = sc.parallelize(data, 13)
    val newRdd = rdd.sortBy((x: Int) => x.toLong)
    assert(newRdd.collect() === data)
  }

}

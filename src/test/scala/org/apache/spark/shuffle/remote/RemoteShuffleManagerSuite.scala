package org.apache.spark.shuffle.remote

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkFunSuite}

class RemoteShuffleManagerSuite extends SparkFunSuite with LocalSparkContext {

  testWithGeneralAndOptimizedShufflePath("repartition") { conf =>
    sc = new SparkContext("local", "test_repartition", conf)
    val data = 1 until 20
    val rdd = sc.parallelize(data, 10)
    val newRdd = rdd.repartition(20)
    assert(newRdd.collect().sorted === data)
  }


  testWithGeneralAndOptimizedShufflePath("repartition with some map output empty") { conf =>
    sc = new SparkContext("local", "test_repartition_empty", conf)
    val data = 1 until 20
    val rdd = sc.parallelize(data, 30)
    val newRdd = rdd.repartition(40)
    assert(newRdd.collect().sorted === data)
  }

  testWithGeneralAndOptimizedShufflePath("sort") { conf =>
    sc = new SparkContext("local", "test", conf)
    val data = 1 until 500
    val rdd = sc.parallelize(data, 13)
    val newRdd = rdd.sortBy((x: Int) => x.toLong)
    assert(newRdd.collect() === data)
  }

  private def testWithGeneralAndOptimizedShufflePath(name: String, loadDefaults: Boolean = true)
      (body: (SparkConf => Unit)): Unit = {
    test(name + " with general shuffle path") {
      RemoteShuffleManager.useOptimizedShuffleWriterThisTime = false
      body(new SparkConf(loadDefaults)
        .set("spark.shuffle.manager", "org.apache.spark.shuffle.remote.RemoteShuffleManager"))
    }
    test(name + " with optimized shuffle path") {
      RemoteShuffleManager.useOptimizedShuffleWriterThisTime = true
      body(new SparkConf(loadDefaults)
        .set("spark.shuffle.manager", "org.apache.spark.shuffle.remote.RemoteShuffleManager"))
    }
  }

}

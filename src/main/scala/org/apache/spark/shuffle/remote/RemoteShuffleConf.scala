package org.apache.spark.shuffle.remote

import org.apache.spark.internal.config.{ConfigBuilder, ConfigEntry}

object RemoteShuffleConf {

  val HDFS_MASTER_URI: ConfigEntry[String] =
    ConfigBuilder("spark.shuffle.remote.hdfsMasterUri")
        .doc("Store shuffle files contacting this HDFS namenode")
        .stringConf
        .createWithDefault("localhost:9001")

}

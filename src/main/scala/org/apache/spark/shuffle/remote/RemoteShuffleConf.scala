package org.apache.spark.shuffle.remote

import org.apache.spark.internal.config.{ConfigBuilder, ConfigEntry}

object RemoteShuffleConf {

  val STORAGE_MASTER_URI: ConfigEntry[String] =
    ConfigBuilder("spark.shuffle.remote.storageMasterUri")
        .doc("Contact this storage master while persisting shuffle files")
        .stringConf
        .createWithDefault("hdfs://localhost:9001")

  val SHUFFLE_FILES_ROOT_DIRECTORY: ConfigEntry[String] =
    ConfigBuilder("spark.shuffle.remote.filesRootDirectory")
        .doc("Use this as the root directory for shuffle files")
        .stringConf
        .createWithDefault("/shuffle")

}

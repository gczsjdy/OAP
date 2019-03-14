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

  val REMOTE_OPTIMIZED_SHUFFLE_ENABLED: ConfigEntry[Boolean] =
    ConfigBuilder("spark.shuffle.remote.optimizedPathEnabled")
        .doc("Enable using unsafe-optimized shuffle writer")
        .internal()
        .booleanConf
        .createWithDefault(true)

  val REMOTE_BYPASS_MERGE_THRESHOLD: ConfigEntry[Int] =
    ConfigBuilder("spark.shuffle.remote.bypassMergeThreshold")
        .doc("Remote shuffle manager uses this threshold to decide using bypass-merge(hash-based)" +
            "shuffle or not, a new configuration is introduced because HDFS poorly handles large" +
            "number of small files, and the bypass-merge shuffle write algorithm may produce" +
            "M * R files as intermediate state. Note that this is compared with M * R, instead of" +
            " R in local file system shuffle manager")
        .intConf
        .createWithDefault(300)

}

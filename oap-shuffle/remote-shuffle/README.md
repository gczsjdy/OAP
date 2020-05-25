# Spark Remote Shuffle Plugin

Remote Shuffle is a Spark Shuffle Data I/O plugin, based on the SortShuffleManager algorithms, saving intermediate data into a Hadoop-compatible file system, as opposed to vanilla Spark's local-disks.
 
A shuffle data I/O plugin transcends a shuffle manger plugin in its cleaner code, better maintainability and less hacking. Under the primitives of shuffle I/O plugin system, shuffle algorithms and storage interaction are decoupled.

This is an essential part of enabling Spark on disaggregated compute and storage architecture.

Note that this plugin runs with a [customized Spark](https://github.com/Intel-bigdata/spark/tree/oap-master), instead of Spark 3.0 official releases, due to upstream Spark 3.0 partially(only write side) supports a shuffle I/O plugin. Turn to [this branch](https://github.com/Intel-bigdata/OAP/tree/branch-0.8-spark-2.4.x) if you require a shuffle manager plugin, or are unable to use this customized Spark.

## Build and Deploy

Build the project using the following command or download the pre-built jar: remote-shuffle-\<version\>.jar. This file needs to
be deployed on every compute node that runs Spark. Manually place it on all nodes or let resource manager do the work.

```
    mvn -DskipTests clean package 
```

## Enable Remote Shuffle

Add the jar files to the classpath of Spark driver and executors: Put the
following configurations in spark-defaults.conf or Spark submit command line arguments. 

Note: For DAOS users, DAOS Hadoop/Java API jars should also be included in the classpath as we leverage DAOS Hadoop filesystem.
    
```
    spark.executor.extraClassPath              /path/to/remote-shuffle-dir/remote-shuffle-<version>.jar
    spark.driver.extraClassPath                /path/to/remote-shuffle-dir/remote-shuffle-<version>.jar
```

Enable the remote shuffle plugin and specify the Hadoop storage system URI holding shuffle data.

```
    spark.shuffle.sort.io.plugin.class         org.apache.spark.shuffle.sort.remote.RemoteHadoopShuffleDataIO
    spark.shuffle.remote.storageMasterUri      daos://default:1 # Or hdfs://namenode:port, file:///my/shuffle/dir
```

## Configurations

Configurations and tuning parameters that change the behavior of remote shuffle. Most of them should work well under default values.

### Shuffle Root Directory

This is to configure the root directory holding remote shuffle files. For each Spark application, a
directory named after application ID is created under this root directory.

```
    spark.shuffle.remote.filesRootDirectory     /shuffle
```

### Index Cache Size

This is to configure the cache size for shuffle index files per executor. Shuffle data includes data files and
index files. An index file is small but will be read many (the number of reducers) times. On a large scale, constantly
reading these small index files from Hadoop Filesystem implementation(i.e. HDFS) is going to cause much overhead and latency. In addition, the shuffle files’
transfer completely relies on the network between compute nodes and storage nodes. But the network inside compute nodes are
not fully utilized. The index cache can eliminate the overhead of reading index files from storage cluster multiple times. By
enabling index file cache, a reduce task fetches them from the remote executors who write them instead of reading from
storage. If the remote executor doesn’t have a desired index file in its cache, it will read the file from storage and cache
it locally. The feature can also be disabled by setting the value to zero.

```
    spark.shuffle.remote.index.cache.size        30m
```

### Number of Threads Reading Data Files

This is one of the parameters influencing shuffle read performance. It is to determine number of threads per executor reading shuffle data files from storage.

```
    spark.shuffle.remote.numReadThreads           5
```

### Number of Threads Transitioning Index Files (when index cache is enabled)

This is one of the parameters influencing shuffle read performance. It is to determine the number of client and server threads that transmit index information from another executor’s cache. It is only valid when the index cache feature is enabled.

```
    spark.shuffle.remote.numIndexReadThreads      3
```

### Configurations fetching port for HDFS

When the backend storage is HDFS, we contact http://$host:$port/conf to fetch configurations. They were not locally loaded because we assume absence of local storage.

```
    spark.shuffle.remote.hdfs.storageMasterUIPort  50070
```

### Inherited Spark Shuffle Configurations

These configurations are inherited from upstream Spark, they are still supported in remote shuffle. More explanations can be found in [Spark core docs](https://spark.apache.org/docs/2.4.4/configuration.html#shuffle-behavior) and [Spark SQL docs](https://spark.apache.org/docs/2.4.4/sql-performance-tuning.html).
```
    spark.shuffle.sort.bypassMergeThreshold        # We encourage you to disable hash-based shuffle by setting this to -1, because sort-based shuffle writer is mostly more efficient than the hash-based one under remote shuffle.
                                                     Hash-based shuffle writer entails a merging process, performing 3x I/Os than total shuffle size: 1 time for read I/Os and 2 times for write I/Os, this becomes a larger overhead under remote shuffle:
                                                     the 3x data size is gone through network, arriving at a remote storage system.
    spark.reducer.maxSizeInFlight
    spark.reducer.maxReqsInFlight
    spark.reducer.maxBlocksInFlightPerAddress
    spark.shuffle.compress
    spark.shuffle.file.buffer
    spark.shuffle.io.maxRetries
    spark.shuffle.io.numConnectionsPerPeer
    spark.shuffle.io.preferDirectBufs
    spark.shuffle.io.retryWait
    spark.shuffle.io.backLog
    spark.shuffle.spill.compress
    spark.shuffle.accurateBlockThreshold
    spark.sql.shuffle.partitions
```

### Deprecated Spark Shuffle Configurations

These configurations are deprecated and will not take effect.
```
    spark.maxRemoteBlockSizeFetchToMem             # As we assume no local disks on compute nodes, shuffle blocks are all fetched to memory
    spark.shuffle.service.enabled                  # All following configurations are related to External Shuffle Service. ESS & remote shuffle cannot be enabled at the same time, as this remote shuffle facility takes over almost all functionalities of ESS.
    spark.shuffle.service.port
    spark.shuffle.service.index.cache.size
    spark.shuffle.maxChunksBeingTransferred
    spark.shuffle.registration.timeout
    spark.shuffle.registration.maxAttempts
```

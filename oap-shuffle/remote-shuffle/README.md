# Spark Remote Shuffle Plugin

Remote Shuffle is a Spark ShuffleManager plugin, shuffling data through a remote Hadoop-compatible file system, as opposed to vanilla Spark's local-disks.

This is an essential part of enabling Spark on disaggregated compute and storage architecture.

## Build and Deploy

Build the project using the following command or download the pre-built jar: remote-shuffle-1.0.jar. This file needs to
be deployed on every compute node that runs Spark. Manually place it on all nodes or let resource manager do the work.

```
    mvn -DskipTests clean package 
```

## Enable Remote Shuffle

Add the jar files to the classpath of the Spark executor and driver: Put the
following configurations in spark-defaults.conf or Spark submit command line arguments. 

Note: For DAOS users, DAOS Hadoop/Java API jars should also be included in the classpath as we leverage DAOS Hadoop filesystem.
    
```
    spark.executor.extraClassPath              /path/to/remote-shuffle-dir/remote-shuffle-<version>.jar
    spark.driver.extraClassPath                /path/to/remote-shuffle-dir/remote-shuffle-<version>.jar
```

Enable the remote shuffle manager and specify the Hadoop storage system URI holding shuffle data.

```
    spark.shuffle.manager                      org.apache.spark.shuffle.remote.RemoteShuffleManager
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

### Bypass-merge-sort Threshold

This threshold is used to decide using bypass-merge(hash-based) shuffle or not. By default we disable(by setting it to -1) 
hash-based shuffle writer in remote shuffle, because when memory is relatively sufficient, sort-based shuffle writer is often more efficient than the hash-based one.
Hash-based shuffle writer entails a merging process, performing 3x I/Os than total shuffle size: 1 time for read I/Os and 2 times for write I/Os, this can be an even larger overhead under remote shuffle:
the 3x shuffle size is gone through network, arriving at a remote storage system.

```
    spark.shuffle.remote.bypassMergeThreshold     -1
```

### Number of Threads Reading Data Files

This is one of the parameters influencing shuffle read performance. It is to determine per executor number of threads reading shuffle data files from Hadoop storage in reduce stage.

### Inherited Spark shuffle configurations

These configurations are inherited from upstream Spark, they are still supported in remote shuffle. More explanations can be found in [Spark docs](https://spark.apache.org/docs/2.4.4/configuration.html#shuffle-behavior).
```
    spark.reducer.maxSizeInFlight
    spark.reducer.maxReqsInFlight
    spark.reducer.maxBlocksInFlightPerAddress
    spark.shuffle.compress
    spark.shuffle.file.buffer
    spark.shuffle.spill.compress
    spark.shuffle.accurateBlockThreshold
```

### Deprecated Spark shuffle configurations

These configurations are deprecated and will not take effect.
```
    spark.shuffle.sort.bypassMergeThreshold
    spark.maxRemoteBlockSizeFetchToMem
```
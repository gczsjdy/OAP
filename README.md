# Spark Remote Shuffle Plugin

Remote Shuffle is a Spark ShuffleManager plugin which writes to a Hadoop compatible file system, instead of local disk, while performing shuffle.

This helps with disaggregated computing nodes and storage nodes.

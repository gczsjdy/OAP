/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.test

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
 * A special [[SparkSession]] prepared for OAP testing.
 */
private[sql] class TestOapSession(sc: SparkContext) extends TestSparkSession(sc) { self =>

  def this(sparkConf: SparkConf) {
    this(
      if (sys.props.get("spark.test.localCluster.enabled") == Some("true")) {
        val numExecutor = 3
        val cores = 5
        val memory = 1024
        new SparkContext(s"local-cluster[$numExecutor, $cores, $memory]",
          "test-cluster-sql-context",
          sparkConf.set("spark.sql.testkey", "true"))
      } else {
        new SparkContext("local[2]",
          "test-sql-context",
          sparkConf.set("spark.sql.testkey", "true"))
      })
  }
}
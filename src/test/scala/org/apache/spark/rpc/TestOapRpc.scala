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

package org.apache.spark.rpc

import org.apache.spark.rpc.OapMessages.{MyDummyMessage, MyDummyMessageWithId}
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.sql.SparkSession

object TestOapRpc {

  val spark = SparkSession.builder().master("spark://localhost:7077").getOrCreate()

  def testSendMessageDriverToExecutor: Unit = {
    OapRpcManagerMaster.send(MyDummyMessage("He is a wanderer"))
  }

  def testSendMessageExecutorToDriver: Unit = {
    // Because we cannot get a CoarseGrainedExecutorBackend on Driver, so this OapRpcManagerSlave
    // is actually on Driver
    OapRpcManagerSlave.registerDriverEndpoint(
      spark.sparkContext.schedulerBackend.asInstanceOf[CoarseGrainedSchedulerBackend]
        .driverEndpoint)
    OapRpcManagerSlave.send(MyDummyMessage("I am a slave"))
  }

  def testSendMessageExecutorToDriverWithStatusKept: Unit = {
    OapRpcManagerSlave.registerDriverEndpoint(
      spark.sparkContext.schedulerBackend.asInstanceOf[CoarseGrainedSchedulerBackend]
        .driverEndpoint)
    OapRpcManagerSlave.send(MyDummyMessageWithId("666", "I am a slave"))
  }

  def testPrintStatusKeeperMap: Unit = {
    OapRpcManagerMaster.printKeptStatus
  }

  def main(args: Array[String]): Unit = {
    // Waiting for ExecutorRegister done
    Thread.sleep(10000)

    testSendMessageDriverToExecutor

    testSendMessageExecutorToDriver

    testSendMessageExecutorToDriverWithStatusKept

    Thread.sleep(3000)
    testPrintStatusKeeperMap

    Thread.sleep(3000)
  }
}

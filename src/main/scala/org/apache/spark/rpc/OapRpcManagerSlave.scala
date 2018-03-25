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

import org.apache.spark.rpc.OapMessages._

private[spark] class OapRpcManagerSlave(
  rpcEnv: RpcEnv, driverEndpoint: RpcEndpointRef, executorId: String) extends OapRpcManager {

  private val slaveEndpoint = rpcEnv.setupEndpoint(
    s"OapRpcManagerSlave_$executorId", new OapRpcManagerSlaveEndpoint(rpcEnv))

  initialize()

  private def initialize() = {
    driverEndpoint.askWithRetry[Boolean](RegisterOapRpcManager(executorId, slaveEndpoint))
  }

  override private[spark] def handle(message: OapMessage): Unit = message match {
    case MyDummyMessage(id, someContent) =>
      logWarning(s"Dummy message received on Executor with id: $id, content: $someContent")
      // Following line is to test sending the same message from executor to Driver
      send(message)
    case _ =>
  }

  override private[spark] def send(message: OapMessage): Unit = slaveEndpoint.send(message)
}

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

import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.rpc.OapMessages.RegisterOapRpcManager

/**
 * BlockManagerMasterEndpoint is an [[ThreadSafeRpcEndpoint]] on the master node to track statuses
 * of all slaves' block managers.
 */
private[spark] class OapRpcManagerMasterEndpoint(
    override val rpcEnv: RpcEnv) extends ThreadSafeRpcEndpoint with Logging {

  // Mapping from executor ID to RpcEndpointRef.
  private[rpc] val rpcEndpointRefByExecutor = new mutable.HashMap[String, RpcEndpointRef]

  logInfo("OapRpcManagerMasterEndpoint up")

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RegisterOapRpcManager(executorId, slaveEndpoint) =>
      context.reply(register(executorId, slaveEndpoint))
  }

  private def register(executorId: String, ref: RpcEndpointRef): Boolean = {
    rpcEndpointRefByExecutor += ((executorId, ref))
    true
  }
}

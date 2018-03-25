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

private[spark] class OapRpcManagerSlave extends
  OapRpcManager[ExecutorToDriverMessage, DriverToExecutorMessage] {

  private var _driverEndpoint: Option[RpcEndpointRef] = None

  private[spark] def registerDriverEndpoint(driverEndpoint: RpcEndpointRef): Unit = {
    _driverEndpoint = Some(driverEndpoint)
  }

  private def handleDummyMessage(message: DummyMessage): Unit = message match {
    case dummyMessage @ MyDummyMessage(id, someContent) =>
      logWarning(s"Dummy message received on Executor with id: $id, content: $someContent")
      // Following line is to test sending the same message from executor to Driver
      send(dummyMessage)
  }

  override private[spark] def handle[DriverToExecutorMessage](message: DriverToExecutorMessage):
    Unit = message match {
    case dummyMessage: DummyMessage => handleDummyMessage(dummyMessage)
    case _ =>
  }

  private def sendDummyMessage(message: DummyMessage): Unit = {
    _driverEndpoint match {
      case Some(driverEndponit) => driverEndponit.send(message)
      case None => throw new IllegalArgumentException("DriverEndpoint Unregistered")
    }
  }

  override private[spark] def send[ExecutorToDriverMessage](message: ExecutorToDriverMessage): Unit
    = message match {
    case dummyMessage: DummyMessage => sendDummyMessage(dummyMessage)
    case _ =>
  }
}

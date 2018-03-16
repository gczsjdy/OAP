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

import org.apache.spark.internal.Logging
import org.apache.spark.rpc.OapMessages._
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend

private[spark] object OapRpcManagerMaster extends Logging {

  private var _scheduler: Option[CoarseGrainedSchedulerBackend] = None

  private[rpc] val statusKeeper = RpcRelatedStatusKeeper

  private[spark] def registerScheduler(schedulerBackend: CoarseGrainedSchedulerBackend): Unit = {
    if (_scheduler.isEmpty) {
      _scheduler = Some(schedulerBackend)
    }
  }

  private def sendMessageToExecutors(message: DriverToExecutorMessage): Unit = {
    _scheduler match {
      case Some(scheduler) =>
        val executorDataMap = scheduler.executorDataMap
        for ((_, executorData) <- executorDataMap) {
          executorData.executorEndpoint.send(message)
        }
      case None => throw new IllegalArgumentException("SchedulerBackend Unregistered")
    }
  }

  private def handleDummyMessage(message: DummyMessage): Unit = message match {
    case dummyMessage @ MyDummyMessage(id, someContent) =>
      logWarning(s"Dummy message received on Driver with id: $id, content: $someContent")
      statusKeeper.dummyStatusAdd(id, someContent)
  }

  private[spark] def handle(message: ExecutorToDriverMessage): Unit = message match {
    case dummyMessage: DummyMessage => handleDummyMessage(dummyMessage)
    case _ =>
  }

  private def sendDummyMessage(message: DummyMessage): Unit = message match {
    case dummyMessage: MyDummyMessage => sendMessageToExecutors(dummyMessage)
  }

  private[spark] def send(message: DriverToExecutorMessage): Unit =
    message match {
    case dummyMessage: DummyMessage => sendDummyMessage(dummyMessage)
    case _ =>
  }
}

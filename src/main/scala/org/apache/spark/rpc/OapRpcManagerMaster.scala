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
import org.apache.spark.scheduler.SchedulerBackend
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend

private[spark] object OapRpcManagerMaster extends OapRpcManagerBase with Logging {

  private var _scheduler: Option[SchedulerBackend] = None

  private val statusKeeper = RpcRelatedStatusKeeper

  def registerScheduler(schedulerBackend: SchedulerBackend): Unit = {
    if (_scheduler.isEmpty) {
      _scheduler = Some(schedulerBackend)
    }
  }

  private def sendMessageToExecutors(message: OapMessage): Unit = {
    _scheduler match {
      case Some(scheduler: CoarseGrainedSchedulerBackend) =>
        val executorDataMap = scheduler.executorDataMap
        for ((_, executorData) <- executorDataMap) {
          executorData.executorEndpoint.send(message)
        }
      case Some(_) => throw new IllegalArgumentException("Not CoarseGrainedSchedulerBackend")
      case None => throw new IllegalArgumentException("SchedulerBackend Unregistered")
    }
  }

  private def handleDummyMessage(message: OapDummyMessage): Unit = message match {
    case DummyMessage(someContent) =>
      logWarning(s"Dummy message received on Driver: $someContent")
    case DummyMessageWithId(executorId, someContent) =>
      logWarning(s"Dummy message from $executorId received on Driver: $someContent")
      statusKeeper.dummyStatusMap += executorId -> someContent
  }

  private def handleCacheMessage(message: OapCacheMessage): Unit = message match {
    case _ => ;
  }

  override def handleOapMessage(message: OapMessage): Unit = message match {
    case dummyMessage: OapDummyMessage => handleDummyMessage(dummyMessage)
    case cacheMessage: OapCacheMessage => handleCacheMessage(cacheMessage)
  }

  private def sendDummyMessage(message: OapDummyMessage): Unit = message match {
    case dummyMessage: DummyMessage => sendMessageToExecutors(dummyMessage)
  }

  override def sendOapMessage(message: OapMessage): Unit = message match {
    case dummyMessage: OapDummyMessage => sendDummyMessage(dummyMessage)
    case cacheMessage: OapCacheMessage => handleCacheMessage(cacheMessage)
  }

  // Just for test/debug
  def printKeptStatus: Unit = {
    statusKeeper.dummyStatusMap.foreach{x => logWarning(x.toString)}
  }
}

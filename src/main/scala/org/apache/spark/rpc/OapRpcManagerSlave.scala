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
import org.apache.spark.sql.execution.datasources.oap.filecache.FiberCacheManager

private[spark] object OapRpcManagerSlave extends OapRpcManagerBase {

  private def handleDummyMessage(message: OapDummyMessage): Unit = message match {
    case DummyMessage(someContent) => logWarning(s"Dummy message on Executor: $someContent")
  }

  private def handleCacheMessage(message: OapCacheMessage): Unit = message match {
    case CacheDrop(indexName) => FiberCacheManager.removeIndexCache(indexName)
  }

  override def handleOapMessage(message: OapMessage): Unit = message match {
    case dummyMessage: OapDummyMessage => handleDummyMessage(dummyMessage)
    case cacheMessage: OapCacheMessage => handleCacheMessage(cacheMessage)
  }

  private def sendDummyMessage(message: OapDummyMessage): Unit = message match {
    case dummyMessage: DummyMessage => ;
  }

  override def sendOapMessage(message: OapMessage): Unit = message match {
    case dummyMessage: OapDummyMessage => sendDummyMessage(dummyMessage)
    case cacheMessage: OapCacheMessage => handleCacheMessage(cacheMessage)
  }
}

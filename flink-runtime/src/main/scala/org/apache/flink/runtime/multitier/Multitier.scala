/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.multitier

import retier._

import org.apache.flink.runtime.multitier.CheckpointResponder.{CheckpointRequestorPeer, CheckpointResponderPeer}
import org.apache.flink.runtime.multitier.KvStateRegistryListener.{KvStateRegistryListenerPeer, KvStateRegistryListeningPeer}
import org.apache.flink.runtime.multitier.PartitionProducerStateChecker.{PartitionProducerStateCheckedPeer, PartitionProducerStateCheckerPeer}
import org.apache.flink.runtime.multitier.ResultPartitionConsumableNotifier.{ResultPartitionConsumableNotifierPeer, ResultPartitionConsumableNotifyeePeer}
import org.apache.flink.runtime.multitier.TaskManager.{JobManagerPeer, TaskManagerPeer}
import org.apache.flink.runtime.multitier.TaskManagerActions.TaskManagerActionsPeer

@multitier
object Multitier {
  trait JobManager extends
      JobManagerPeer with
      CheckpointRequestorPeer with
      ResultPartitionConsumableNotifyeePeer with
      PartitionProducerStateCheckedPeer with
      KvStateRegistryListeningPeer {
    type Connection <: Multiple[TaskManager] with
      Multiple[TaskManagerPeer] with
      Multiple[CheckpointResponderPeer] with
      Multiple[ResultPartitionConsumableNotifierPeer] with
      Multiple[PartitionProducerStateCheckerPeer] with
      Multiple[KvStateRegistryListenerPeer]
  }

  trait TaskManager extends
      TaskManagerPeer with
      TaskManagerActionsPeer with
      CheckpointResponderPeer with
      ResultPartitionConsumableNotifierPeer with
      PartitionProducerStateCheckerPeer with
      KvStateRegistryListenerPeer {
    type Connection <: Single[JobManager] with
      Single[JobManagerPeer] with
      Single[TaskManagerActionsPeer] with
      Single[CheckpointRequestorPeer] with
      Single[ResultPartitionConsumableNotifyeePeer] with
      Single[PartitionProducerStateCheckedPeer] with
      Single[KvStateRegistryListeningPeer]
  }
}

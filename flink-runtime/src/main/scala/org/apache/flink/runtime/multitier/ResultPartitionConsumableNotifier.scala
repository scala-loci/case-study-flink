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

import loci._
import loci.basicTransmitter._
import org.apache.flink.multitier._

import org.apache.flink.api.common.JobID
import org.apache.flink.runtime.io.network.partition
import org.apache.flink.runtime.io.network.partition.ResultPartitionID
import org.apache.flink.runtime.taskmanager.TaskActions
import org.slf4j.{Logger, LoggerFactory}
import scala.concurrent.ExecutionContext.Implicits.global

@multitier
object ResultPartitionConsumableNotifier {
  trait ResultPartitionConsumableNotifyeePeer extends Peer {
    type Tie <: Multiple[ResultPartitionConsumableNotifierPeer]
    def notifyPartitionConsumable(
      jobId: JobID,
      partitionId: ResultPartitionID): Any
  }

  trait ResultPartitionConsumableNotifierPeer extends Peer {
    type Tie <: Single[ResultPartitionConsumableNotifyeePeer]
    def resultPartitionConsumableNotifierCreated(
      resultPartitionConsumableNotifier: partition.ResultPartitionConsumableNotifier): Unit
  }

  val LOG: Logger localOn ResultPartitionConsumableNotifierPeer =
    LoggerFactory.getLogger(classOf[ResultPartitionConsumableNotifierPeer])

  placed[ResultPartitionConsumableNotifierPeer] { implicit! =>
    peer resultPartitionConsumableNotifierCreated new partition.ResultPartitionConsumableNotifier {
      def notifyPartitionConsumable(
          jobId: JobID,
          partitionId: ResultPartitionID,
          taskActions: TaskActions) =
        remote[ResultPartitionConsumableNotifyeePeer].capture(jobId, partitionId){ implicit! =>
          peer.notifyPartitionConsumable(jobId, partitionId)
        }.asLocal.failed foreach { failure =>
          LOG.error("Could not schedule or update consumers at the JobManager.", failure)
          taskActions.failExternally(new RuntimeException(
            "Could not notify JobManager to schedule or update consumers",
            failure))
        }
    }
  }
}

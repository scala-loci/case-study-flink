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
import loci.contexts.Immediate.Implicits.global
import loci.transmitter.basic._
import org.apache.flink.multitier._

import akka.actor.{ActorRef, ActorSystem, Status}
import org.apache.flink.api.common.JobID
import org.apache.flink.api.common.time.Time
import org.apache.flink.runtime.blob.BlobKey
import org.apache.flink.runtime.checkpoint.CheckpointOptions
import org.apache.flink.runtime.clusterframework.ApplicationStatus
import org.apache.flink.runtime.concurrent.impl.FlinkFuture
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor
import org.apache.flink.runtime.executiongraph.{ExecutionAttemptID, PartitionInfo}
import org.apache.flink.runtime.instance.{ActorGateway, AkkaActorGateway, InstanceID}
import org.apache.flink.runtime.jobmanager.slots.{ActorTaskManagerGateway, TaskManagerGateway}
import org.apache.flink.runtime.messages.{Acknowledge, StackTrace, StackTraceSampleMessages, StackTraceSampleResponse}
import org.apache.flink.runtime.messages.TaskManagerMessages.{LogFileRequest, LogTypeRequest, StdOutFileRequest}
import org.apache.flink.util.Preconditions
import scala.concurrent.Future
import scala.util.{Failure, Success}

@multitier
object TaskManager {
  trait JobManagerPeer extends Peer {
    type Tie <: Multiple[TaskManagerPeer]
    val actorSystem: ActorSystem
    def taskManagerGatewayCreated(taskManagerGateway: TaskManagerGateway): Unit
    val createTaskManagerGateway: Notification[ActorGateway]
  }

  trait TaskManagerPeer extends Peer {
    type Tie <: Single[JobManagerPeer]
    def submitTask(tdd: TaskDeploymentDescriptor): Either[Acknowledge, Status.Failure]
    def stopTask(executionAttemptID: ExecutionAttemptID): Either[Acknowledge, Status.Failure]
    def cancelTask(executionAttemptID: ExecutionAttemptID): Acknowledge
    def updatePartitions(
      executionAttemptID: ExecutionAttemptID,
      partitionInfos: java.lang.Iterable[PartitionInfo]): Either[Acknowledge, Status.Failure]
    def failPartition(executionAttemptID: ExecutionAttemptID): Unit
    def notifyCheckpointComplete(
      executionAttemptID: ExecutionAttemptID,
      jobId: JobID,
      checkpointId: Long,
      timestamp: Long): Unit
    def triggerCheckpoint(
      executionAttemptID: ExecutionAttemptID,
      jobId: JobID,
      checkpointId: Long,
      timestamp: Long,
      checkpointOptions: CheckpointOptions): Unit
    def requestTaskManagerLog(logTypeRequest: LogTypeRequest): Either[BlobKey, Status.Failure]
    def disconnectFromJobManager(instanceId: InstanceID, cause: Exception): Unit
    def stopCluster(applicationStatus: ApplicationStatus, message: String): Unit
    def requestStackTrace(): Option[StackTrace]
  }

  def remoteResult[T](actorRef: ActorRef)(body: Remote[TaskManagerPeer] => Future[T]) =
    placed[JobManagerPeer].local { implicit! =>
      flinkRemoteResult(actorRef, remote[TaskManagerPeer].connected)(body)(peer.actorSystem)
    }

  def remoteMessage(actorRef: ActorRef)(body: Remote[TaskManagerPeer] => Unit) =
    placed[JobManagerPeer].local { implicit! =>
      flinkRemoteMessage(actorRef, remote[TaskManagerPeer].connected)(body)(peer.actorSystem)
    }

  placed[JobManagerPeer] { implicit! =>
    implicit val actorSystem = peer.actorSystem

    peer.createTaskManagerGateway notify { actorGateway =>
      peer taskManagerGatewayCreated new ActorTaskManagerGateway(actorGateway) {
        override def disconnectFromJobManager(instanceId: InstanceID, cause: Exception) = {
          remoteMessage(actorGateway.actor) { taskManager =>
            remote.on(taskManager).capture(instanceId, cause){ implicit! =>
              peer.disconnectFromJobManager(instanceId, cause)
            }
          }
        }

        override def stopCluster(applicationStatus: ApplicationStatus, message: String) = {
          remoteMessage(actorGateway.actor) { taskManager =>
            remote.on(taskManager).capture(applicationStatus, message){ implicit! =>
              peer.stopCluster(applicationStatus, message)
            }
          }
        }

        override def requestStackTrace(timeout: Time) = {
          Preconditions.checkNotNull(timeout)

          remoteResult(actorGateway.actor) { taskManager =>
            remote.on(taskManager){ implicit! =>
              peer.requestStackTrace()
            }.asLocal.timeout(timeout.asFiniteDuration).map(_.get)
          }
        }

        override def requestStackTraceSample(
            executionAttemptID: ExecutionAttemptID,
            sampleId: Int,
            numSamples: Int,
            delayBetweenSamples: Time,
            maxStackTraceDepth: Int,
            timeout: Time) = {
          Preconditions.checkNotNull(executionAttemptID)
          Preconditions.checkArgument(numSamples > 0,
            "The number of samples must be greater than 0.": Object)
          Preconditions.checkNotNull(delayBetweenSamples)
          Preconditions.checkArgument(maxStackTraceDepth >= 0,
            "The max stack trace depth must be greater or equal than 0.": Object)
          Preconditions.checkNotNull(timeout)

          val stackTraceSampleResponseFuture = actorGateway.ask(
            StackTraceSampleMessages.TriggerStackTraceSample(
              sampleId,
              executionAttemptID,
              numSamples,
              delayBetweenSamples,
              maxStackTraceDepth),
            timeout.asFiniteDuration)
            .timeout(timeout.asFiniteDuration)
            .flatMap { result =>
              Future fromTry (result match {
                case response: StackTraceSampleResponse => Success(response)
                case Status.Failure(exception) => Failure(exception)
              })
            }

          new FlinkFuture(stackTraceSampleResponseFuture)
        }

        override def submitTask(tdd: TaskDeploymentDescriptor, timeout: Time) = {
          Preconditions.checkNotNull(tdd)
          Preconditions.checkNotNull(timeout)

          remoteResult(actorGateway.actor) { taskManager =>
            remote.on(taskManager).capture(tdd){ implicit! =>
              peer.submitTask(tdd)
            }.asLocal.timeout(timeout.asFiniteDuration).map(_.left.get)
          }
        }

        override def stopTask(executionAttemptID: ExecutionAttemptID, timeout: Time) = {
          Preconditions.checkNotNull(executionAttemptID)
          Preconditions.checkNotNull(timeout)

          remoteResult(actorGateway.actor) { taskManager =>
            remote.on(taskManager).capture(executionAttemptID){ implicit! =>
              peer.stopTask(executionAttemptID)
            }.asLocal.timeout(timeout.asFiniteDuration).map(_.left.get)
          }
        }

        override def cancelTask(executionAttemptID: ExecutionAttemptID, timeout: Time) = {
          Preconditions.checkNotNull(executionAttemptID)
          Preconditions.checkNotNull(timeout)

          remoteResult(actorGateway.actor) { taskManager =>
            remote.on(taskManager).capture(executionAttemptID){ implicit! =>
              peer.cancelTask(executionAttemptID)
            }.asLocal.timeout(timeout.asFiniteDuration)
          }
        }

        override def updatePartitions(
            executionAttemptID: ExecutionAttemptID,
            partitionInfos: java.lang.Iterable[PartitionInfo], timeout: Time) = {
          Preconditions.checkNotNull(executionAttemptID)
          Preconditions.checkNotNull(partitionInfos)

          remoteResult(actorGateway.actor) { taskManager =>
            remote.on(taskManager).capture(executionAttemptID, partitionInfos){ implicit! =>
              peer.updatePartitions(executionAttemptID, partitionInfos)
            }.asLocal.map(_.left.get)
          }
        }

        override def failPartition(executionAttemptID: ExecutionAttemptID) = {
          Preconditions.checkNotNull(executionAttemptID)

          remoteMessage(actorGateway.actor) { taskManager =>
            remote.on(taskManager).capture(executionAttemptID){ implicit! =>
              peer.failPartition(executionAttemptID)
            }
          }
        }

        override def notifyCheckpointComplete(
            executionAttemptID: ExecutionAttemptID,
            jobId: JobID,
            checkpointId: Long,
            timestamp: Long) = {
          Preconditions.checkNotNull(executionAttemptID)
          Preconditions.checkNotNull(jobId)

          remoteMessage(actorGateway.actor) { taskManager =>
            remote.on(taskManager).capture(
                executionAttemptID, jobId, checkpointId, timestamp){ implicit! =>
              peer.notifyCheckpointComplete(executionAttemptID, jobId, checkpointId, timestamp)
            }
          }
        }

        override def triggerCheckpoint(
            executionAttemptID: ExecutionAttemptID,
            jobId: JobID,
            checkpointId: Long,
            timestamp: Long,
            checkpointOptions: CheckpointOptions) = {
          Preconditions.checkNotNull(executionAttemptID)
          Preconditions.checkNotNull(jobId)

          remoteMessage(actorGateway.actor) { taskManager =>
            remote.on(taskManager).capture(
                executionAttemptID, jobId, checkpointId, timestamp, checkpointOptions){ implicit! =>
              peer.triggerCheckpoint(
                executionAttemptID, jobId, checkpointId, timestamp, checkpointOptions)
            }
          }
        }

        override def requestTaskManagerLog(timeout: Time) = {
          Preconditions.checkNotNull(timeout)

          remoteResult(actorGateway.actor) { taskManager =>
            remote.on(taskManager){ implicit! =>
              peer.requestTaskManagerLog(LogFileRequest)
            }.asLocal.timeout(timeout.asFiniteDuration).map(_.left.get)
          }
        }

        override def requestTaskManagerStdout(timeout: Time) = {
          Preconditions.checkNotNull(timeout)

          remoteResult(actorGateway.actor) { taskManager =>
            remote.on(taskManager){ implicit! =>
              peer.requestTaskManagerLog(StdOutFileRequest)
            }.asLocal.timeout(timeout.asFiniteDuration).map(_.left.get)
          }
        }
      }
    }
  }
}

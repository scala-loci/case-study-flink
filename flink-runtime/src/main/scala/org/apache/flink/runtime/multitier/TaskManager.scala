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

@multitier trait TaskManager {
  @peer type JobManager <: { type Tie <: Multiple[TaskManager] }
  @peer type TaskManager <: { type Tie <: Single[JobManager] }

  implicit val actorSystem: Local[ActorSystem] on JobManager

  def submitTask(
    tdd: TaskDeploymentDescriptor): Either[Acknowledge, Status.Failure] on TaskManager

  def stopTask(
    executionAttemptID: ExecutionAttemptID): Either[Acknowledge, Status.Failure] on TaskManager

  def cancelTask(
    executionAttemptID: ExecutionAttemptID): Acknowledge on TaskManager

  def updatePartitions(
    executionAttemptID: ExecutionAttemptID,
    partitionInfos: java.lang.Iterable[PartitionInfo])
  : Either[Acknowledge, Status.Failure] on TaskManager

  def failPartition(executionAttemptID: ExecutionAttemptID): Unit on TaskManager

  def notifyCheckpointComplete(
    executionAttemptID: ExecutionAttemptID,
    jobId: JobID,
    checkpointId: Long,
    timestamp: Long): Unit on TaskManager

  def triggerCheckpoint(
    executionAttemptID: ExecutionAttemptID,
    jobId: JobID,
    checkpointId: Long,
    timestamp: Long,
    checkpointOptions: CheckpointOptions): Unit on TaskManager

  def requestTaskManagerLog(
    logTypeRequest: LogTypeRequest)
  : Either[BlobKey, Status.Failure] on TaskManager

  def disconnectFromJobManager(instanceId: InstanceID, cause: Exception): Unit on TaskManager

  def stopCluster(applicationStatus: ApplicationStatus, message: String): Unit on TaskManager

  def requestStackTrace(): Option[StackTrace] on TaskManager

  def remoteResult[T](actorRef: ActorRef)(body: Remote[TaskManager] => Future[T]) =
    on[JobManager] local {
      flinkRemoteResult(actorRef, remote[TaskManager].connected)(body)
    }

  def remoteMessage(actorRef: ActorRef)(body: Remote[TaskManager] => Unit) =
    on[JobManager] local {
      flinkRemoteMessage(actorRef, remote[TaskManager].connected)(body)
    }

  def createTaskManagerGateway(actorGateway: ActorGateway)
  : Local[ActorTaskManagerGateway] on JobManager =
    on[JobManager] local {
      new ActorTaskManagerGateway(actorGateway) {
        override def disconnectFromJobManager(instanceId: InstanceID, cause: Exception) = {
          remoteMessage(actorGateway.actor) { taskManager =>
            remote(taskManager) call TaskManager.this.disconnectFromJobManager(instanceId, cause)
          }
        }

        override def stopCluster(applicationStatus: ApplicationStatus, message: String) = {
          remoteMessage(actorGateway.actor) { taskManager =>
            remote(taskManager) call TaskManager.this.stopCluster(applicationStatus, message)
          }
        }

        override def requestStackTrace(timeout: Time) = {
          Preconditions.checkNotNull(timeout)

          remoteResult(actorGateway.actor) { taskManager =>
            (remote(taskManager) call TaskManager.this.requestStackTrace())
              .asLocal.timeout(timeout.asFiniteDuration).map(_.get)
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
            (remote(taskManager) call TaskManager.this.submitTask(tdd))
              .asLocal.timeout(timeout.asFiniteDuration).map(_.left.get)
          }
        }

        override def stopTask(executionAttemptID: ExecutionAttemptID, timeout: Time) = {
          Preconditions.checkNotNull(executionAttemptID)
          Preconditions.checkNotNull(timeout)

          remoteResult(actorGateway.actor) { taskManager =>
            (remote(taskManager) call TaskManager.this.stopTask(executionAttemptID))
              .asLocal.timeout(timeout.asFiniteDuration).map(_.left.get)
          }
        }

        override def cancelTask(executionAttemptID: ExecutionAttemptID, timeout: Time) = {
          Preconditions.checkNotNull(executionAttemptID)
          Preconditions.checkNotNull(timeout)

          remoteResult(actorGateway.actor) { taskManager =>
            (remote(taskManager) call TaskManager.this.cancelTask(executionAttemptID))
              .asLocal.timeout(timeout.asFiniteDuration)
          }
        }

        override def updatePartitions(
            executionAttemptID: ExecutionAttemptID,
            partitionInfos: java.lang.Iterable[PartitionInfo], timeout: Time) = {
          Preconditions.checkNotNull(executionAttemptID)
          Preconditions.checkNotNull(partitionInfos)

          remoteResult(actorGateway.actor) { taskManager =>
            (remote(taskManager) call TaskManager.this.updatePartitions(
                executionAttemptID, partitionInfos))
              .asLocal.map(_.left.get)
          }
        }

        override def failPartition(executionAttemptID: ExecutionAttemptID) = {
          Preconditions.checkNotNull(executionAttemptID)

          remoteMessage(actorGateway.actor) { taskManager =>
            remote(taskManager) call TaskManager.this.failPartition(executionAttemptID)
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
            remote(taskManager) call TaskManager.this.notifyCheckpointComplete(
              executionAttemptID, jobId, checkpointId, timestamp)
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
            remote(taskManager) call TaskManager.this.triggerCheckpoint(
              executionAttemptID, jobId, checkpointId, timestamp, checkpointOptions)
          }
        }

        override def requestTaskManagerLog(timeout: Time) = {
          Preconditions.checkNotNull(timeout)

          remoteResult(actorGateway.actor) { taskManager =>
            (remote(taskManager) call TaskManager.this.requestTaskManagerLog(LogFileRequest))
              .asLocal.timeout(timeout.asFiniteDuration).map(_.left.get)
          }
        }

        override def requestTaskManagerStdout(timeout: Time) = {
          Preconditions.checkNotNull(timeout)

          remoteResult(actorGateway.actor) { taskManager =>
            (remote(taskManager) call TaskManager.this.requestTaskManagerLog(StdOutFileRequest))
              .asLocal.timeout(timeout.asFiniteDuration).map(_.left.get)
          }
        }
      }
    }
}

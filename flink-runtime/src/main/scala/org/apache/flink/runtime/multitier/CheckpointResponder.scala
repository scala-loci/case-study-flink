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
import org.apache.flink.multitier._

import org.apache.flink.api.common.JobID
import org.apache.flink.runtime.checkpoint.{CheckpointMetrics, SubtaskState}
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID
import org.apache.flink.runtime.taskmanager

@multitier trait CheckpointResponder {
  @peer type JobManager <: { type Tie <: Multiple[TaskManager] }
  @peer type TaskManager <: { type Tie <: Single[JobManager] }

  def acknowledgeCheckpoint(
    jobID: JobID,
    executionAttemptID: ExecutionAttemptID,
    checkpointId: Long,
    checkpointMetrics: CheckpointMetrics,
    checkpointStateHandles: SubtaskState): Unit on JobManager

  def declineCheckpoint(
    jobID: JobID,
    executionAttemptID: ExecutionAttemptID,
    checkpointId: Long,
    reason: Throwable): Unit on JobManager

  val checkpointResponder = on[TaskManager] local { implicit! =>
    new taskmanager.CheckpointResponder {
      def acknowledgeCheckpoint(
          jobID: JobID,
          executionAttemptID: ExecutionAttemptID,
          checkpointId: Long,
          checkpointMetrics: CheckpointMetrics,
          checkpointStateHandles: SubtaskState) =
        remote call CheckpointResponder.this.acknowledgeCheckpoint(jobID, executionAttemptID,
          checkpointId, checkpointMetrics, checkpointStateHandles)

      def declineCheckpoint(
          jobID: JobID,
          executionAttemptID: ExecutionAttemptID,
          checkpointId: Long,
          reason: Throwable) =
        remote call CheckpointResponder.this.declineCheckpoint(jobID, executionAttemptID,
          checkpointId, reason)
    }
  }
}

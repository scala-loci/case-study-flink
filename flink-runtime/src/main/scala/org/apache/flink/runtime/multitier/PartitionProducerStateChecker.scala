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

import akka.actor.Status
import org.apache.flink.api.common.JobID
import org.apache.flink.runtime.concurrent.Future
import org.apache.flink.runtime.concurrent.impl.FlinkFuture
import org.apache.flink.runtime.execution.ExecutionState
import org.apache.flink.runtime.io.network.netty
import org.apache.flink.runtime.io.network.partition.ResultPartitionID
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID

@multitier trait PartitionProducerStateChecker {
  @peer type JobManager <: { type Tie <: Multiple[TaskManager] }
  @peer type TaskManager <: { type Tie <: Single[JobManager] }

  def requestPartitionProducerState(
    jobId: JobID,
    intermediateDataSetId: IntermediateDataSetID,
    resultPartitionId: ResultPartitionID)
  : Either[ExecutionState, Status.Failure] on JobManager

  val partitionProducerStateChecker = on[TaskManager] local {
    new netty.PartitionProducerStateChecker {
      def requestPartitionProducerState(
          jobId: JobID,
          intermediateDataSetId: IntermediateDataSetID,
          resultPartitionId: ResultPartitionID): Future[ExecutionState] = new FlinkFuture(
        (remote call PartitionProducerStateChecker.this.requestPartitionProducerState(jobId,
          intermediateDataSetId, resultPartitionId)).asLocal.map(_.left.get))
    }
  }
}

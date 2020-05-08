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
import org.apache.flink.runtime.jobgraph.JobVertexID
import org.apache.flink.runtime.query
import org.apache.flink.runtime.query.{KvStateID, KvStateServerAddress}
import org.apache.flink.runtime.state.KeyGroupRange

@multitier trait KvStateRegistryListener {
  @peer type JobManager <: { type Tie <: Multiple[TaskManager] }
  @peer type TaskManager <: { type Tie <: Single[JobManager] }

  def notifyKvStateRegistered(
    jobId: JobID,
    jobVertexId: JobVertexID,
    keyGroupRange: KeyGroupRange,
    registrationName: String,
    kvStateId: KvStateID,
    kvStateServerAddress: KvStateServerAddress): Unit on JobManager

  def notifyKvStateUnregistered(
    jobId: JobID,
    jobVertexId: JobVertexID,
    keyGroupRange: KeyGroupRange,
    registrationName: String): Unit on JobManager

  def createKvStateRegistryListener(kvStateServerAddress: KvStateServerAddress) =
    on[TaskManager] local {
      new query.KvStateRegistryListener {
        def notifyKvStateRegistered(
            jobId: JobID,
            jobVertexId: JobVertexID,
            keyGroupRange: KeyGroupRange,
            registrationName: String,
            kvStateId: KvStateID) =
          remote call KvStateRegistryListener.this.notifyKvStateRegistered(
              jobId, jobVertexId, keyGroupRange, registrationName, kvStateId, kvStateServerAddress)

        def notifyKvStateUnregistered(
            jobId: JobID,
            jobVertexId: JobVertexID,
            keyGroupRange: KeyGroupRange,
            registrationName: String) =
          remote call KvStateRegistryListener.this.notifyKvStateUnregistered(jobId, jobVertexId,
            keyGroupRange, registrationName)
      }
    }
}

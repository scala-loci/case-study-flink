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
import loci.util.Notification
import org.apache.flink.multitier._

import org.apache.flink.api.common.JobID
import org.apache.flink.runtime.jobgraph.JobVertexID
import org.apache.flink.runtime.query
import org.apache.flink.runtime.query.{KvStateID, KvStateServerAddress}
import org.apache.flink.runtime.state.KeyGroupRange

@multitier
object KvStateRegistryListener {
  trait KvStateRegistryListeningPeer extends Peer {
    type Tie <: Multiple[KvStateRegistryListenerPeer]
    def notifyKvStateRegistered(
      jobId: JobID,
      jobVertexId: JobVertexID,
      keyGroupRange: KeyGroupRange,
      registrationName: String,
      kvStateId: KvStateID,
      kvStateServerAddress: KvStateServerAddress): Unit
    def notifyKvStateUnregistered(
      jobId: JobID,
      jobVertexId: JobVertexID,
      keyGroupRange: KeyGroupRange,
      registrationName: String): Unit
  }

  trait KvStateRegistryListenerPeer extends Peer {
    type Tie <: Single[KvStateRegistryListeningPeer]
    def kvStateRegistryListenerCreated(kvStateRegistryListener: query.KvStateRegistryListener): Unit
    val createKvStateRegistryListener: Notification[KvStateServerAddress]
  }

  placed[KvStateRegistryListenerPeer] { implicit! =>
    peer.createKvStateRegistryListener += { kvStateServerAddress =>
      peer kvStateRegistryListenerCreated new query.KvStateRegistryListener {
        def notifyKvStateRegistered(
            jobId: JobID,
            jobVertexId: JobVertexID,
            keyGroupRange: KeyGroupRange,
            registrationName: String,
            kvStateId: KvStateID) =
          remote[KvStateRegistryListeningPeer].capture(
              jobId, jobVertexId, keyGroupRange, registrationName,
              kvStateId, kvStateServerAddress){ implicit! =>
            peer.notifyKvStateRegistered(
              jobId, jobVertexId, keyGroupRange, registrationName, kvStateId, kvStateServerAddress)
          }

        def notifyKvStateUnregistered(
            jobId: JobID,
            jobVertexId: JobVertexID,
            keyGroupRange: KeyGroupRange,
            registrationName: String) =
          remote[KvStateRegistryListeningPeer].capture(
              jobId, jobVertexId, keyGroupRange, registrationName){ implicit! =>
            peer.notifyKvStateUnregistered(jobId, jobVertexId, keyGroupRange, registrationName)
          }
      }
    }
  }
}

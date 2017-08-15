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
import org.apache.flink.multitier._

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID
import org.apache.flink.runtime.taskmanager
import org.apache.flink.runtime.taskmanager.TaskExecutionState

@multitier
object TaskManagerActions {
  trait TaskManagerActionsPeer extends Peer {
    type Connection <: Single[TaskManagerActionsPeer]
    def taskManagerActionsCreated(taskManagerActions: taskmanager.TaskManagerActions): Unit

    def notifyFinalState(executionAttemptID: ExecutionAttemptID): Unit
    def notifyFatalError(message: String, cause: Throwable): Unit
    def failTask(executionAttemptID: ExecutionAttemptID, cause: Throwable): Unit
    def updateTaskExecutionState(taskExecutionState: TaskExecutionState): Unit
  }

  placed[TaskManagerActionsPeer] { implicit! =>
    peer taskManagerActionsCreated new taskmanager.TaskManagerActions {
      def notifyFinalState(executionAttemptID: ExecutionAttemptID) =
        remote[TaskManagerActionsPeer].capture(executionAttemptID){ implicit! =>
          peer.notifyFinalState(executionAttemptID)
        }

      def notifyFatalError(message: String, cause: Throwable) =
        remote[TaskManagerActionsPeer].capture(message, cause){ implicit! =>
          peer.notifyFatalError(message, cause)
        }

      def failTask(executionAttemptID: ExecutionAttemptID, cause: Throwable) =
        remote[TaskManagerActionsPeer].capture(executionAttemptID, cause){ implicit! =>
          peer.failTask(executionAttemptID, cause)
        }

      def updateTaskExecutionState(taskExecutionState: TaskExecutionState) =
        remote[TaskManagerActionsPeer].capture(taskExecutionState){ implicit! =>
          peer.updateTaskExecutionState(taskExecutionState)
        }
    }
  }
}

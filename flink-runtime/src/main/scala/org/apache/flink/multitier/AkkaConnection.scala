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

package org.apache.flink.multitier

import loci._
import loci.communicator._
import loci.contexts.Immediate.Implicits.global

import akka.actor.{Actor, ActorRef}
import java.util.UUID
import scala.collection.mutable
import scala.concurrent.Promise
import scala.util.{Success, Try}

class AkkaEnpoint(val setup: ConnectionSetup[AkkaEnpoint], val actorRef: ActorRef)
    extends Protocol with SetupInfo with SecurityInfo with SymmetryInfo with Bidirectional {
  val encrypted = false
  val integrityProtected = false
  val authenticated = false
}

case class AkkaMultitierMessage(data: Option[(String, UUID)])

class AkkaConnection(val protocol: AkkaEnpoint, leaderSessionID: UUID)(
  implicit val sender: ActorRef = Actor.noSender)
    extends Connection[AkkaEnpoint] {
  private var isOpen = true
  private val doClosed = Notice.Steady[Unit]
  private val doReceive = Notice.Stream[MessageBuffer]

  val closed = doClosed.notice
  val receive = doReceive.notice

  def open = isOpen

  def send(data: MessageBuffer) =
    protocol.actorRef ! AkkaMultitierMessage(Some((data.decodeString, leaderSessionID)))

  def close() = {
    protocol.actorRef ! AkkaMultitierMessage(None)
    isOpen = false
    doClosed.set()
  }

  def process(message: AkkaMultitierMessage, leaderSessionID: Option[UUID]) = message.data match {
    case Some((data, uuid)) =>
      leaderSessionID foreach { leaderSessionID =>
        if (leaderSessionID == uuid && open) {
          doReceive fire (MessageBuffer encodeString data)
        }
      }
    case None =>
      isOpen = false
      doClosed.set()
  }
}

class AkkaConnector extends Connector[AkkaEnpoint] {
  private val connection = Notice.Steady[Try[AkkaConnection]]

  def connect(connectionEstablished: Connected[AkkaEnpoint]) =
    connection.notice foreach connectionEstablished.set

  def newConnection(actorRef: ActorRef, leaderSessionID: UUID)(
      implicit sender: ActorRef = Actor.noSender) =
    connection.set(Success(new AkkaConnection(new AkkaEnpoint(this, actorRef), leaderSessionID)))

  def process(message: AkkaMultitierMessage, leaderSessionID: Option[UUID]) =
    connection.notice.current foreach { _ foreach { _ process (message, leaderSessionID) } }
}

class AkkaListener extends Listener[AkkaEnpoint] {
  var connected = Option.empty[Connected[AkkaEnpoint]]

  val connections = mutable.Map.empty[ActorRef, AkkaConnection]

  protected def startListening(connectionEstablished: Connected[AkkaEnpoint]) = {
    connected = Some(connectionEstablished)
    Success(new Listening { def stopListening() = () })
  }

  def newConnection(actorRef: ActorRef, leaderSessionID: UUID)(
      implicit sender: ActorRef = Actor.noSender) = {
    val connection = new AkkaConnection(new AkkaEnpoint(this, actorRef), leaderSessionID)
    connection.closed foreach { _ => connections -= actorRef }
    connections += actorRef -> connection
    connected foreach { _ fire Success(connection) }
  }

  def process(actorRef: ActorRef, message: AkkaMultitierMessage, leaderSessionID: Option[UUID]) =
    connections get actorRef foreach { _ process (message, leaderSessionID) }
}

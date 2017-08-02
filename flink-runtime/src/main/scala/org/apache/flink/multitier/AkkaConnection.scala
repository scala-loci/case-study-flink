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

import retier.network._
import retier.util.Notifier

import akka.actor.{Actor, ActorRef}
import scala.collection.mutable
import scala.concurrent.Promise

class AkkaEnpoint(val establisher: ConnectionEstablisher, val actorRef: ActorRef)
    extends ProtocolInfo {
  def isEncrypted = false
  def isProtected = false
  def isAuthenticated = false
  def identification = None
}

case class AkkaMultitierMessage(data: String)

class AkkaConnectionRequestor extends ConnectionRequestor {
  private val doClosed = Notifier[Unit]
  private val doReceive = Notifier[String]

  private val promise = Promise[Connection]

  def request = promise.future

  def newConnection(actorRef: ActorRef)(implicit sender: ActorRef = Actor.noSender) = {
    promise success new Connection {
      val protocol = new AkkaEnpoint(AkkaConnectionRequestor.this, actorRef)
      val closed = doClosed.notification
      val receive = doReceive.notification

      def isOpen = true

      def send(data: String) =
        actorRef ! AkkaMultitierMessage(data)

      def close() = { }
    }
  }

  def process(message: AkkaMultitierMessage) =
    doReceive(message.data)
}

class AkkaConnectionListener extends ConnectionListener {
  def start() = { }

  def stop() = { }

  val connections = mutable.Map.empty[ActorRef, Notifier[String]]

  def newConnection(actorRef: ActorRef)(implicit sender: ActorRef = Actor.noSender) = {
    val doClosed = Notifier[Unit]
    val doReceive = Notifier[String]

    connections += actorRef -> doReceive

    doConnectionEstablished(new Connection {
      val protocol = new AkkaEnpoint(AkkaConnectionListener.this, actorRef)
      val closed = doClosed.notification
      val receive = doReceive.notification

      def isOpen = true

      def send(data: String) =
        actorRef ! AkkaMultitierMessage(data)

      def close() = { }
    })
  }

  def process(actorRef: ActorRef, message: AkkaMultitierMessage) =
    connections get actorRef foreach { _(message.data) }
}

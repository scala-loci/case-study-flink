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

package org.apache.flink

import loci._
import loci.transmitter.{IdenticallyTransmittable, Serializable, Transmittable,Transmittables}

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.after
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.util.Base64
import org.apache.flink.api.common.time.Time
import org.apache.flink.runtime.concurrent.impl.FlinkFuture
import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Future, TimeoutException}
import scala.reflect.ClassTag
import scala.util.Try

package object multitier {
  implicit def transmittableAny[T]: Transmittable[T, T, T] {
    type Proxy = Future[T]
    type Transmittables = Transmittables.None
  } = IdenticallyTransmittable()

  implicit def serializableAny[T: ClassTag] = new Serializable[T] {
    def serialize(value: T) = {
      val arrayStream = new ByteArrayOutputStream
      val objectStream = new ObjectOutputStream(arrayStream)
      objectStream writeObject value
      MessageBuffer fromString (Base64.getEncoder encodeToString arrayStream.toByteArray)
    }

    def deserialize(value: MessageBuffer) = Try {
      (implicitly[ClassTag[T]].runtimeClass cast
        new ObjectInputStream(
          new ByteArrayInputStream(
            Base64.getDecoder decode
              (value.toString(0, value.length)).getBytes)).readObject).asInstanceOf[T]
    }
  }

  implicit class FutureTimeoutOp[T](future: Future[T]) {
    def timeout(duration: FiniteDuration)(implicit actorSystem: ActorSystem) = {
      implicit val dispatcher = actorSystem.dispatcher
      Future firstCompletedOf Seq(
        future,
        after(
          duration, actorSystem.scheduler)(
          Future.failed(new TimeoutException("Future timed out"))))
    }
  }

  implicit class TimeAsDurationOp(time: Time) {
    def asFiniteDuration = FiniteDuration(time.getSize, time.getUnit)
  }

  def findRemote[P](remotes: Traversable[Remote[P]], actorRef: ActorRef) =
    remotes collectFirst {
      case remote
        if (remote.protocol match {
          case protocol: AkkaEnpoint => protocol.actorRef == actorRef
          case _ => false
        }) => remote
    }

  def flinkRemoteResult[P, T](actorRef: ActorRef, remotes: => Traversable[Remote[P]])(
      body: Remote[P] => Future[T])(implicit actorSystem: ActorSystem) = {
    val promise = scala.concurrent.Promise[T]

    def runWithRemote(): Unit =
      findRemote(remotes, actorRef) match {
        case Some(remote) =>
          promise completeWith body(remote)
        case None =>
          actorSystem.scheduler.scheduleOnce(1.millisecond)(runWithRemote)(actorSystem.dispatcher)
      }

    runWithRemote()

    new FlinkFuture(promise.future)
  }

  def flinkRemoteMessage[P](actorRef: ActorRef, remotes: => Traversable[Remote[P]])(
      body: Remote[P] => Unit)(implicit actorSystem: ActorSystem) = {
    def runWithRemote(): Unit =
      findRemote(remotes, actorRef) match {
        case Some(remote) =>
          body(remote)
        case None =>
          actorSystem.scheduler.scheduleOnce(1.millisecond)(runWithRemote)(actorSystem.dispatcher)
      }

    runWithRemote()
  }
}

/**
 * Copyright (C) 2018 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import scala.util.Random

import akka.actor.CoordinatedShutdown.ClusterLeavingReason
import akka.actor._
import com.typesafe.config._

sealed trait Msg;
case object Ping extends Msg;
case object Pong extends Msg;
case object Leak extends Msg;

// FIXME remove this file

class ServerActor extends Actor {
  import context._

  def receive = {
    case Leak ⇒
      val worker = system.actorOf(Props[WorkerActor])
      worker.tell(Ping, sender)

    case Ping ⇒
      sender ! Pong
  }
}

class WorkerActor extends Actor {
  import context._

  var arr: Array[Byte] = _

  def receive = {
    case Ping ⇒
      arr = Array.fill(1024 * 1024)(0)
      sender ! Pong
      stop(self)
  }
}

class ClientActor(msg: Msg) extends Actor with ActorLogging {
  import context._

  val identifyId = new Random().nextLong()

  override def preStart: Unit = {
    actorSelection("akka://Cluster@127.0.0.1:2552/user/server") ! Identify(identifyId)
  }

  def receive = {
    case ActorIdentity(`identifyId`, None) ⇒
      log.warning("Server not discovered")

    case ActorIdentity(`identifyId`, Some(actorRef)) ⇒
      actorRef ! msg

    case Pong ⇒
      stop(self)
      //system.terminate()
      // leaving cluster
      CoordinatedShutdown(system).run(ClusterLeavingReason)
  }
}

object Cfg {
  def config =
    ConfigFactory.parseString("""
      | akka {
      |   loglevel = "DEBUG"
      |   actor.provider = cluster
      |   remote.artery {
      |     enabled = on
      |     transport = tcp
      |     canonical.port = 0
      |     canonical.hostname = 127.0.0.1
      |     log-sent-messages = on
      |     advanced.stop-idle-outbound-after = 1 minutes
      |     advanced.compression {
      |       actor-refs.max = 256
      |       actor-refs.advertisement-interval = 10s
      |       manifests.max = 256
      |       manifests.advertisement-interval = 10s
      |     }
      |   }
      |   cluster {
      |     seed-nodes = ["akka://Cluster@127.0.0.1:2552"]
      |     auto-down-unreachable-after = 1s
      |   }
      | }
      |""".stripMargin).withFallback(ConfigFactory.load())

  def leakyServerConfig =
    ConfigFactory
      .parseString("akka.remote.artery.canonical.port = 2552")
      .withFallback(config)

  def nonLeakyServerConfig =
    ConfigFactory
      .parseString("""
        | akka.remote.artery {
        |   canonical.port = 2552
        |   canonical.hostname = 127.0.0.1
        |   advanced {
        |     compression {
        |       actor-refs.max = 0
        |       manifests.max = 0
        |     }
        |   }
        | }
        """.stripMargin).withFallback(config)
}

trait Server {
  def cfg: Config

  def main(args: Array[String]): Unit = {
    val system = ActorSystem("Cluster", cfg)
    val server = system.actorOf(Props[ServerActor], name = "server")
  }
}

object LeakyServer extends Server {
  val cfg = Cfg.leakyServerConfig
}

object NonLeakyServer extends Server {
  val cfg = Cfg.nonLeakyServerConfig
}

trait Client {
  import scala.concurrent.Await
  import scala.concurrent.duration.Duration

  import Cfg._

  val msg: Msg

  def main(args: Array[String]): Unit = {
    (1 to 1).foreach { n ⇒
      val system = ActorSystem("Cluster", config)

      while (!Cluster(system).state.members.exists(_.uniqueAddress == Cluster(system).selfUniqueAddress)) {
        Thread.sleep(1000)
      }

      val client = system.actorOf(Props(new ClientActor(msg)), name = "client")
      Await.result(system.whenTerminated, Duration.Inf)
    }
  }
}

object PingClient extends Client {
  val msg = Ping
}

object LeakClient extends Client {
  val msg = Leak
}

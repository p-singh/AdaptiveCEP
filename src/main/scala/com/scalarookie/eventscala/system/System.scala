package com.scalarookie.eventscala.system

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout
import com.scalarookie.eventscala.data.Events.{DependenciesRequest, DependenciesResponse, Event, GraphCreated}
import com.scalarookie.eventscala.data.Queries._
import com.scalarookie.eventscala.graph._
import com.scalarookie.eventscala.graph.nodes._
import com.scalarookie.eventscala.graph.qos._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

object System {
  val index = new AtomicInteger(0)
}

class System(implicit actorSystem: ActorSystem) {
  private val roots = ListBuffer.empty[ActorRef]
  private val placements = mutable.Map.empty[ActorRef, Host] withDefaultValue NoHost

  private val frequencyMonitorFactory = AveragedFrequencyMonitorFactory(interval = 15, logging = true)
  private val latencyMonitorFactory = PathLatencyMonitorFactory(interval = 10, logging = true)

  def runQuery(query: Query, publishers: Map[String, ActorRef], callback: Option[Either[GraphCreated.type, Event] => Any]) =
    roots += actorSystem.actorOf(Props(query match {
      case streamQuery: StreamQuery =>
        StreamNode(streamQuery, publishers, frequencyMonitorFactory, latencyMonitorFactory, callback)
      case filterQuery: FilterQuery =>
        FilterNode(filterQuery, publishers, frequencyMonitorFactory, latencyMonitorFactory, callback)
      case selectQuery: SelectQuery =>
        SelectNode(selectQuery, publishers, frequencyMonitorFactory, latencyMonitorFactory, callback)
      case selfJoinQuery: SelfJoinQuery =>
        SelfJoinNode(selfJoinQuery, publishers, frequencyMonitorFactory, latencyMonitorFactory, callback)
      case joinQuery: JoinQuery =>
        JoinNode(joinQuery, publishers, frequencyMonitorFactory, latencyMonitorFactory, callback)
    }), s"root-${System.index.getAndIncrement()}")

  def consumers: Seq[Operator] = {
    import actorSystem.dispatcher
    implicit val timeout = Timeout(20.seconds)

    def operator(actorRef: ActorRef): Future[Operator] =
      actorRef ? DependenciesRequest flatMap {
        case DependenciesResponse(dependencies) =>
          Future sequence (dependencies map operator) map {
            ActorOperator(placements(actorRef), _, actorRef)
          }
      }

    Await result (Future sequence (roots map operator), timeout.duration)
  }

  def place(operator: Operator, host: Host) = {
    val ActorOperator(_, _, actorRef) = operator
    placements += actorRef -> host
  }

  private case class ActorOperator(host: Host, dependencies: Seq[Operator], actorRef: ActorRef) extends Operator
}

package adaptivecep.graph.nodes

import akka.actor.{Actor, ActorRef, Props}
import adaptivecep.data.Queries._
import adaptivecep.graph.qos.MonitorFactory

trait Node extends Actor {

  val name: String = self.path.name

  val query: Query
  val publishers: Map[String, ActorRef]
  val frequencyMonitorFactory: MonitorFactory
  val latencyMonitorFactory: MonitorFactory

  def createChildNode(
      id: Int,
      query: Query
    ): ActorRef = query match {
    case streamQuery: StreamQuery =>
      context.actorOf(Props(StreamNode(streamQuery, publishers, frequencyMonitorFactory, latencyMonitorFactory, None)), s"$name-$id-stream")
    case filterQuery: FilterQuery =>
      context.actorOf(Props(FilterNode(filterQuery, publishers, frequencyMonitorFactory, latencyMonitorFactory, None)), s"$name-$id-filter")
    case selectQuery: SelectQuery =>
      context.actorOf(Props(SelectNode(selectQuery, publishers, frequencyMonitorFactory, latencyMonitorFactory, None)), s"$name-$id-select")
    case selfJoinQuery: SelfJoinQuery =>
      context.actorOf(Props(SelfJoinNode(selfJoinQuery, publishers, frequencyMonitorFactory, latencyMonitorFactory, None)), s"$name-$id-selfjoin")
    case joinQuery: JoinQuery =>
      context.actorOf(Props(JoinNode(joinQuery, publishers, frequencyMonitorFactory, latencyMonitorFactory, None)), s"$name-$id-join")
  }

}

package adaptivecep.graph.qos

import java.util.concurrent.TimeUnit

import akka.actor.ActorContext
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.FiniteDuration
import adaptivecep.data.Events.Event
import adaptivecep.data.Queries._

trait AveragedFrequencyMonitor {

  val logging: Boolean
  val interval: Int

  var currentOutput: Option[Int] = None

  def onCreated(nodeName: String, query: Query, context: ActorContext): Unit = {
    val requirements: Set[FrequencyRequirement] = query.requirements.collect{ case fr: FrequencyRequirement => fr }
    currentOutput = Some(0)
    if (requirements.nonEmpty) {
      context.system.scheduler.schedule(
        initialDelay = FiniteDuration(interval, TimeUnit.SECONDS),
        interval = FiniteDuration(interval, TimeUnit.SECONDS),
        runnable = () => {
          requirements.foreach(requirement => {
            require(requirement.seconds <= interval)
            // `divisor`, e.g., if `interval` == 30, and `requirement.seconds` == 10, then `divisor` == 3
            val divisor: Int = interval / requirement.seconds
            val frequency: Int = currentOutput.get / divisor
            if (logging) println(
              s"FREQUENCY:\tOn average, node `$nodeName` emits $frequency events every ${requirement.seconds} seconds." +
              s"(Calculated every $interval seconds.)")
            requirement.operator match {
              case Equal =>        if (!(frequency == requirement.instances)) requirement.callback(nodeName)
              case NotEqual =>     if (!(frequency != requirement.instances)) requirement.callback(nodeName)
              case Greater =>      if (!(frequency >  requirement.instances)) requirement.callback(nodeName)
              case GreaterEqual => if (!(frequency >= requirement.instances)) requirement.callback(nodeName)
              case Smaller =>      if (!(frequency <  requirement.instances)) requirement.callback(nodeName)
              case SmallerEqual => if (!(frequency <= requirement.instances)) requirement.callback(nodeName)
            }
          })
          currentOutput = Some(0)
        }
      )
    }
  }

  def onEventEmit(event: Event): Unit = {
    if (currentOutput.isDefined) currentOutput = Some(currentOutput.get + 1)
  }

}

case class AveragedFrequencyLeafNodeMonitor(interval: Int, logging: Boolean) extends AveragedFrequencyMonitor with LeafNodeMonitor {

  override def onCreated(nodeData: LeafNodeData): Unit = onCreated(nodeData.name, nodeData.query, nodeData.context)
  override def onEventEmit(event: Event, nodeData: LeafNodeData): Unit = onEventEmit(event)

}

case class AveragedFrequencyUnaryNodeMonitor(interval: Int, logging: Boolean) extends AveragedFrequencyMonitor with UnaryNodeMonitor {

  override def onCreated(nodeData: UnaryNodeData): Unit = onCreated(nodeData.name, nodeData.query, nodeData.context)
  override def onEventEmit(event: Event, nodeData: UnaryNodeData): Unit = onEventEmit(event)

}

case class AveragedFrequencyBinaryNodeMonitor(interval: Int, logging: Boolean) extends AveragedFrequencyMonitor with BinaryNodeMonitor {

  override def onCreated(nodeData: BinaryNodeData): Unit = onCreated(nodeData.name, nodeData.query, nodeData.context)
  override def onEventEmit(event: Event, nodeData: BinaryNodeData): Unit = onEventEmit(event)

}

case class AveragedFrequencyMonitorFactory(interval: Int, logging: Boolean) extends MonitorFactory {

  override def createLeafNodeMonitor: LeafNodeMonitor = AveragedFrequencyLeafNodeMonitor(interval, logging)
  override def createUnaryNodeMonitor: UnaryNodeMonitor = AveragedFrequencyUnaryNodeMonitor(interval, logging)
  override def createBinaryNodeMonitor: BinaryNodeMonitor = AveragedFrequencyBinaryNodeMonitor(interval, logging)

}

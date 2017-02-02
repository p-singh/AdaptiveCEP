package adaptivecep.graph.nodes

import akka.actor.ActorRef
import com.espertech.esper.client._
import adaptivecep.data.Events._
import adaptivecep.data.Queries._
import adaptivecep.graph.qos._
import JoinNode._

object JoinNode {

  def createArrayOfNames(query: Query): Array[String] = query match {
    case _: Query1[_] => Array("e1")
    case _: Query2[_, _] => Array("e1", "e2")
    case _: Query3[_, _, _] => Array("e1", "e2", "e3")
    case _: Query4[_, _, _, _] => Array("e1", "e2", "e3", "e4")
    case _: Query5[_, _, _, _, _] => Array("e1", "e2", "e3", "e4", "e5")
    case _: Query6[_, _, _, _, _, _] => Array("e1", "e2", "e3", "e4", "e5", "e6")
  }

  def createArrayOfClasses(query: Query): Array[Class[_]] = {
    val clazz: Class[_] = classOf[AnyRef]
    query match {
      case _: Query1[_] => Array(clazz)
      case _: Query2[_, _] => Array(clazz, clazz)
      case _: Query3[_, _, _] => Array(clazz, clazz, clazz)
      case _: Query4[_, _, _, _] => Array(clazz, clazz, clazz, clazz)
      case _: Query5[_, _, _, _, _] => Array(clazz, clazz, clazz, clazz, clazz)
      case _: Query6[_, _, _, _, _, _] => Array(clazz, clazz, clazz, clazz, clazz, clazz)
    }
  }

  def castToAnyRef(any: Any): AnyRef = {
    // Yep, you can do that!
    // https://stackoverflow.com/questions/25931611/why-anyval-can-be-converted-into-anyref-at-run-time-in-scala
    any.asInstanceOf[AnyRef]
  }

  def createWindowEplString(window: Window): String = window match {
    case SlidingInstances(instances) => s"win:length($instances)"
    case TumblingInstances(instances) => s"win:length_batch($instances)"
    case SlidingTime(seconds) => s"win:time($seconds)"
    case TumblingTime(seconds) => s"win:time_batch($seconds)"
  }

}

case class JoinNode(
    query: JoinQuery,
    publishers: Map[String, ActorRef],
    frequencyMonitorFactory: MonitorFactory,
    latencyMonitorFactory: MonitorFactory,
    callbackIfRoot: Option[Either[GraphCreated.type, Event] => Any])
  extends Node with EsperEngine {

  val childNode1: ActorRef = createChildNode(1, query.sq1)
  val childNode2: ActorRef = createChildNode(2, query.sq2)

  var graphCreatedFromChildNode1: Boolean = false
  var graphCreatedFromChildNode2: Boolean = false

  val nodeData: BinaryNodeData = BinaryNodeData(name, query, context, childNode1, childNode2)

  val frequencyMonitor: BinaryNodeMonitor = frequencyMonitorFactory.createBinaryNodeMonitor
  val latencyMonitor: BinaryNodeMonitor = latencyMonitorFactory.createBinaryNodeMonitor
  //val frequencyReqs: Set[FrequencyRequirement] = query.requirements collect { case fr: FrequencyRequirement => fr }
  //val latencyReqs: Set[LatencyRequirement] = query.requirements collect { case lr: LatencyRequirement => lr }

  override val esperServiceProviderUri: String = name

  def emitGraphCreated(): Unit = {
    if (callbackIfRoot.isDefined) callbackIfRoot.get.apply(Left(GraphCreated)) else context.parent ! GraphCreated
    frequencyMonitor.onCreated(nodeData)
    latencyMonitor.onCreated(nodeData)
  }

  def emitEvent(event: Event): Unit = {
    if (callbackIfRoot.isDefined) callbackIfRoot.get.apply(Right(event)) else context.parent ! event
    frequencyMonitor.onEventEmit(event, nodeData)
    latencyMonitor.onEventEmit(event, nodeData)
  }

  override def receive: Receive = {
    case DependenciesRequest =>
      sender ! DependenciesResponse(Seq(childNode1, childNode2))
    case GraphCreated if sender() == childNode1 =>
      graphCreatedFromChildNode1 = true
      if (graphCreatedFromChildNode2) emitGraphCreated()
    case GraphCreated if sender() == childNode2 =>
      graphCreatedFromChildNode2 = true
      if (graphCreatedFromChildNode1) emitGraphCreated()
    case event: Event if sender() == childNode1 => event match {
      case Event1(e1) => sendEvent("sq1", Array(castToAnyRef(e1)))
      case Event2(e1, e2) => sendEvent("sq1", Array(castToAnyRef(e1), castToAnyRef(e2)))
      case Event3(e1, e2, e3) => sendEvent("sq1", Array(castToAnyRef(e1), castToAnyRef(e2), castToAnyRef(e3)))
      case Event4(e1, e2, e3, e4) => sendEvent("sq1", Array(castToAnyRef(e1), castToAnyRef(e2), castToAnyRef(e3), castToAnyRef(e4)))
      case Event5(e1, e2, e3, e4, e5) => sendEvent("sq1", Array(castToAnyRef(e1), castToAnyRef(e2), castToAnyRef(e3), castToAnyRef(e4), castToAnyRef(e5)))
      case Event6(e1, e2, e3, e4, e5, e6) => sendEvent("sq1", Array(castToAnyRef(e1), castToAnyRef(e2), castToAnyRef(e3), castToAnyRef(e4), castToAnyRef(e5), castToAnyRef(e6)))
    }
    case event: Event if sender() == childNode2 => event match {
      case Event1(e1) => sendEvent("sq2", Array(castToAnyRef(e1)))
      case Event2(e1, e2) => sendEvent("sq2", Array(castToAnyRef(e1), castToAnyRef(e2)))
      case Event3(e1, e2, e3) => sendEvent("sq2", Array(castToAnyRef(e1), castToAnyRef(e2), castToAnyRef(e3)))
      case Event4(e1, e2, e3, e4) => sendEvent("sq2", Array(castToAnyRef(e1), castToAnyRef(e2), castToAnyRef(e3), castToAnyRef(e4)))
      case Event5(e1, e2, e3, e4, e5) => sendEvent("sq2", Array(castToAnyRef(e1), castToAnyRef(e2), castToAnyRef(e3), castToAnyRef(e4), castToAnyRef(e5)))
      case Event6(e1, e2, e3, e4, e5, e6) => sendEvent("sq2", Array(castToAnyRef(e1), castToAnyRef(e2), castToAnyRef(e3), castToAnyRef(e4), castToAnyRef(e5), castToAnyRef(e6)))
    }
    case unhandledMessage =>
      frequencyMonitor.onMessageReceive(unhandledMessage, nodeData)
      latencyMonitor.onMessageReceive(unhandledMessage, nodeData)
  }

  override def postStop(): Unit = {
    destroyServiceProvider()
  }

  addEventType("sq1", createArrayOfNames(query.sq1), createArrayOfClasses(query.sq1))
  addEventType("sq2", createArrayOfNames(query.sq2), createArrayOfClasses(query.sq2))

  val epStatement: EPStatement = createEpStatement(
    s"select * from " +
    s"sq1.${createWindowEplString(query.w1)} as sq1, " +
    s"sq2.${createWindowEplString(query.w2)} as sq2")

  val updateListener: UpdateListener = (newEventBeans: Array[EventBean], _) => newEventBeans.foreach(eventBean => {
    val values: Array[Any] =
      eventBean.get("sq1").asInstanceOf[Array[Any]] ++
      eventBean.get("sq2").asInstanceOf[Array[Any]]
    val event: Event = values.length match {
      case 2 => Event2(values(0), values(1))
      case 3 => Event3(values(0), values(1), values(2))
      case 4 => Event4(values(0), values(1), values(2), values(3))
      case 5 => Event5(values(0), values(1), values(2), values(3), values(4))
      case 6 => Event6(values(0), values(1), values(2), values(3), values(4), values(5))
    }
    emitEvent(event)
  })

  epStatement.addListener(updateListener)

}

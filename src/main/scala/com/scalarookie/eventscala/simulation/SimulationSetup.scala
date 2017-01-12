package com.scalarookie.eventscala.simulation

import java.io.PrintStream

import akka.actor.{ActorSystem, Props}
import com.scalarookie.eventscala.data.Queries.Query
import com.scalarookie.eventscala.dsl.Dsl._
import com.scalarookie.eventscala.publishers.EmptyPublisher
import com.scalarookie.eventscala.simulation.SimulationSetup._
import com.scalarookie.eventscala.system.System

object SimulationSetup {
  implicit val actorSystem = ActorSystem()

  val publishers = Map(
    "A" -> actorSystem.actorOf(Props(new EmptyPublisher), "A"),
    "B" -> actorSystem.actorOf(Props(new EmptyPublisher), "B"),
    "C" -> actorSystem.actorOf(Props(new EmptyPublisher), "C"))

  val query0: Query =
    stream[Int]("A")
      .join(
        stream[Int]("B"),
        slidingWindow(30.seconds),
        slidingWindow(30.seconds))
      .keepEventsWith(_ <= _)
      .removeElement1()

  val query1: Query =
    stream[Int]("A")
      .join(
        stream[Int]("B"),
        slidingWindow(30.seconds),
        slidingWindow(30.seconds))
      .keepEventsWith(_ <= _)
      .join(
        stream[Int]("C")
          .keepEventsWith(0 <= _),
        slidingWindow(30.seconds),
        slidingWindow(30.seconds))
      .removeElement1()

  val queries = Seq("query0" -> query0, "query1" -> query1)
}

class SimulationSetup(queries: Query*)(out: PrintStream)(optimize: (Simulation, Long, Long, Long) => Unit) {
  def run() = {
    val steps = 3000
    val outputSampleSeconds = 10

    def createSimulation = {
      val system = new System
      queries foreach { system runQuery(_, publishers, None) }

      val simulation = new Simulation(system)
      simulation.placeSequentially()

      simulation
    }

    val simulationStatic = createSimulation
    val simulationAdaptive = createSimulation

    optimize(simulationAdaptive, 0, Int.MaxValue, 0)
    optimize(simulationAdaptive, 0, Int.MaxValue, 0)

    out.println("time-s,latencystatic-ms,latencyadaptive-ms,bandwidthstatic,bandwidthadaptive")

    0 to steps foreach { step =>
      val time = simulationStatic.currentTime.toSeconds
      val simulationStaticLatency = simulationStatic.measureLatency.toMillis
      val simulationAdaptiveLatency = simulationAdaptive.measureLatency.toMillis
      val simulationStaticBandwidth = simulationStatic.measureBandwidth.toLong
      val simulationAdaptiveBandwidth = simulationAdaptive.measureBandwidth.toLong

      if ((time % outputSampleSeconds) == 0)
        out.println(Seq(time, simulationStaticLatency, simulationAdaptiveLatency, simulationStaticBandwidth, simulationAdaptiveBandwidth) mkString ",")

      optimize(simulationAdaptive, time, simulationAdaptiveLatency, simulationAdaptiveBandwidth)

      simulationStatic.advance()
      simulationAdaptive.advance()
    }
  }
}

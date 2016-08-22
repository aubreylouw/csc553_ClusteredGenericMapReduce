package clusteredmapreduce

import akka.actor.{ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import akka.cluster.client.{ClusterClient,ClusterClientReceptionist}

object MapReduceClusterApp {
  // initialization for testing purposes
  def main(args: Array[String]): Unit = {
    val listenerConfig = ConfigFactory.parseString("akka.remote.netty.tcp.port=" + "2551").
      withFallback(ConfigFactory.parseString("akka.cluster.roles = [receptionist]")).
      withFallback(ConfigFactory.load())
    val receptionSystem = ActorSystem("MapReduceCluster", listenerConfig)
    val receptionist = receptionSystem.actorOf(Props[MapReduceServiceReceptionist], name = "receptionist")
    ClusterClientReceptionist(receptionSystem).registerService(receptionist)

    val serviceConfig = ConfigFactory.parseString("akka.remote.netty.tcp.port=" + "2552").
      withFallback(ConfigFactory.parseString("akka.cluster.roles = [service]")).
      withFallback(ConfigFactory.load())
    ActorSystem("MapReduceCluster", serviceConfig).actorOf(Props[MapReduceService], name = "service")

    // spin up workers ; default of 2
    var numWorkers = 2
    val portNumber = "2552"
    if (!args.isEmpty && args(0).toInt > 0) numWorkers = args(0).toInt
    for { index <- 1 until numWorkers+1} {
      val thisWorkerPortNumber = portNumber.toInt + index
      val workerConfig = ConfigFactory.parseString("akka.remote.netty.tcp.port=" + thisWorkerPortNumber.toString).
        withFallback(ConfigFactory.parseString("akka.cluster.roles = [worker]")).
        withFallback(ConfigFactory.load())
      ActorSystem("MapReduceCluster", workerConfig).actorOf(Props[MapReduceWorker], name = "worker"+index.toString)
    }
  }
}
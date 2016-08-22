package mapreduceclient

import akka.actor.{ActorPath, ActorSystem, Props}
import akka.cluster.client.{ClusterClient, ClusterClientSettings}

object MapReduceClientApp {

  val wordCountFiles = List[String](
    "CSC536 Homework1_ael.txt", "CSC536 Homework2_ael.txt",
    "CSC536 Homework4_ael.txt", "CSC536 Homework5_ael.txt",
    "CSC536 Homework6_ael.txt")

  val reverseIndexFiles = List[String](
    "CSC536 Homework1_ael.txt", "CSC536 Homework2_ael.txt",
    "CSC536 Homework4_ael.txt", "CSC536 Homework5_ael.txt",
    "CSC536 Homework6_ael.txt")

  val pageRankFiles = List[String]("one.html", "two.html", "three.html", "four.html", "five.html")

  // the implementation to run for this client
  var clientImpl: Option[clientImplementations.clientImplementationsTrait] = Option(null)

  // initialization for testing purposes
  def main(args: Array[String]): Unit = {
	 
    // basic validity check on input
    if (args.isEmpty || args.size != 2)
      throw new IllegalArgumentException("Requires 2 parameters <1-3 map-reduce use case, path for resource files>")

    // match input to a client implementation and pass the files mapped to a base file path
    args(0) match {
      case "1" => clientImpl = clientImplementations.
        getImplementation("word_count", wordCountFiles.map(x=>args(1)+"\\"+x))
      case "2" => clientImpl = clientImplementations.
        getImplementation("reverse_index", reverseIndexFiles.map(x=>args(1)+"\\"+x))
      case "3" => clientImpl = clientImplementations.
        getImplementation("pagerank", pageRankFiles.map(x=>args(1)+"\\"+x))
    }

    // basic validity check on input
    if (!clientImpl.isDefined) throw new IllegalArgumentException("Select a value between 1-3. 1 = Word Count ; 2 = Reverse Index ; 3 = First step in PageRank")

    // start up the actor system
    val clientSystem = ActorSystem("MapReduceClient")
    val mapReduceSystemContacts = Set(ActorPath.fromString("akka.tcp://MapReduceCluster@127.0.0.1:2551/system/receptionist"))
    val mapReduceSystem = clientSystem.actorOf(ClusterClient.props(ClusterClientSettings(clientSystem).withInitialContacts(mapReduceSystemContacts)))

    val clientSystemActor = clientSystem.actorOf(Props(new MapReduceClient(mapReduceSystem, "/user/receptionist")), name = "clientActor")

    clientSystemActor ! clientImpl.get
  }
}



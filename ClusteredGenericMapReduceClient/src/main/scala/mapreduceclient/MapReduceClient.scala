package mapreduceclient

import akka.actor.{Actor, ActorLogging, ActorRef}

import scala.collection.immutable.HashMap
import akka.cluster.client.ClusterClient
import implementations._
import messages._

class MapReduceClient(server: ActorRef, relativePath:String) extends Actor with ActorLogging {

  // stores a {jobID, the results}
  var jobRegister: HashMap[Int, Seq[String]] = HashMap.empty

  override def receive: Receive = {
    case clientImplementations.WORD_COUNT(files) => {
      val job = MapReduceJobRequest(files,
        wordCountDataFetchFuncFactory,
        wordCountMapFuncFactory,
        wordCountReduceFuncFactory);

      server ! ClusterClient.Send(relativePath, MapReduceJobRequest(files,
        wordCountDataFetchFuncFactory,
        wordCountMapFuncFactory,
        wordCountReduceFuncFactory), localAffinity = true)
      println("send WORD_COUNT request")
    }

    case clientImplementations.REVERSE_INDEX(files) => {
      val job = MapReduceJobRequest(files,
        reverseIndexDataFetchFuncFactory,
        reverseIndexMapFuncFactory,
        reverseIndexReduceFuncFactory);

      server ! ClusterClient.Send(relativePath, job, localAffinity = true)
    }

    case clientImplementations.PAGERANK(files) => {
      val job = MapReduceJobRequest(files,
        countIncidentEdgesDataFetchFuncFactory,
        countIncidentEdgesMapFuncFactory,
        countIncidentEdgesReduceFuncFactory);

      server ! ClusterClient.Send(relativePath, job, localAffinity = true)
    }

    case MapReduceJobFailure(jobID, reason) => {
      println(s"Job $jobID failed! Reason: $reason")
      println("----------------------------------")
    }

    case MapReduceJobComplete(jobID, results) =>  {
      jobRegister = jobRegister.+((jobID, results))
      val numRecords = results.size
      println(s"Job $jobID completed successfully with a result set of size $numRecords")
      println("----------------------------------")
      results.foreach(println)
    }

    case MapReduceJobConfirmation(jobID) => {
      println(s"Received confirmation that $jobID is in progress.")
      jobRegister = jobRegister.+((jobID, List.empty))
    }
  }
}
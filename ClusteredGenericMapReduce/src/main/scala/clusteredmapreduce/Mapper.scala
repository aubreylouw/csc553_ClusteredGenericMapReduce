package clusteredmapreduce

import java.io.FileNotFoundException

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSelection, OneForOneStrategy, Props}
import scala.collection.immutable.HashMap
import akka.actor.SupervisorStrategy.{Directive, Escalate, Stop}
import messages._

class Mapper extends Actor with ActorLogging{

  // the variables associated with a job ID
  // _1 => jobID
  // _2 => data fecth operation
  // _3 => map operation
  // _4 => the service actor
  var jobVars: HashMap[Int, (DataFetchFunction, MapFunction, ActorRef)] = HashMap.empty
  var jobDataFetchers: HashMap[ActorRef, Int] = HashMap.empty

  override val supervisorStrategy = OneForOneStrategy() {
    case _: FileNotFoundException =>


      log.error("[" + akka.serialization.Serialization.serializedActorPath(self) + "] logged an EXCEPTION from " +
        sender.path.toStringWithoutAddress)

      val jobID = jobDataFetchers.get(sender()).get
      self ! MapOperationFailure(jobID, "FileNotFoundException")
      Stop
  }

  override def receive: Receive = {

    // internal message from data fetch actor
    // (1) map the contents
    // (2) send the mapped data to the reducer router
    // (3) send a success message to the map reduce service
    case SourceDocumentContents(jobID, title, contents) => {

      log.info("[" + akka.serialization.Serialization.serializedActorPath(self) + "] received SourceDocument#" +
        jobID + "-" + title + " with " + contents.size + " items")

      val mapOp: MapFunction = jobVars.get(jobID).get._2
      val serviceActor: ActorRef = jobVars.get(jobID).get._3

      mapOp.map(title, contents)
      mapOp.get().foreach(entry => {
        //println(entry)
        val key = entry._1
        serviceActor ! MapOperationEmit(jobID, key, List(entry))
      })
      //serviceActor ! MapOperationEmit(jobID, title, mapOp.get())
      self ! MapOperationComplete(jobID)
      // no need to continue tracking this child actor
      // jobDataFetchers.-=(sender)
    }

    case MapOperationFailure(jobID, reason) => {
      log.error("[" + akka.serialization.Serialization.serializedActorPath(self) + "] received MapOperationFailure#" + jobID + " due to " + reason)
      val serviceActor: ActorRef = jobVars.get(jobID).get._3
      serviceActor ! MapOperationFailure(jobID, reason)
    }

    case MapOperationComplete(jobID) => {
      val serviceActor: ActorRef = jobVars.get(jobID).get._3
      serviceActor ! MapOperationComplete(jobID)
    }

    // create a child to fetch the source data
    // the child will reply with the document contents
    case MapOperation(jobID, resource, dataFetchOp, mapOp) => {

      val source: ActorRef = sender()
      val thisJobVars = (dataFetchOp, mapOp, source)

      log.info("[" + akka.serialization.Serialization.serializedActorPath(self) + "] received MapOperation#" + jobID + " from " + source.path)

      jobVars = jobVars + ((jobID, thisJobVars))

      // create a child actor to handle the IO
      val docFetcher = context.actorOf(Props[SourceDocumentFetcherActor])
      jobDataFetchers.+=((docFetcher, jobID))
      docFetcher ! SourceDocument(jobID, dataFetchOp, resource, resource)
    }
  }
}

// internal messages between a mapper and its children
private case class SourceDocument(jobID:Int, dataFetchOp: DataFetchFunction, title:String, location:String)
private case class SourceDocumentContents(jobID:Int, title:String, contents:String)

private class SourceDocumentFetcherActor extends Actor {

  def receive = {
    case SourceDocument(jobID, dataFetchOp, title, location) => {
      sender() ! SourceDocumentContents(jobID, title, dataFetchOp.fetch(location))
    }
  }
}

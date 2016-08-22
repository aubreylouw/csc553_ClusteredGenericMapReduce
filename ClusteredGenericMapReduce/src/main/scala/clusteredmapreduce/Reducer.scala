package clusteredmapreduce

import akka.actor.{Actor, ActorLogging, ActorRef}
import scala.collection.immutable.{HashMap, Stack}
import messages._

class Reducer extends Actor with ActorLogging {

  // the variables associated with a job ID
  // _1 reduce func
  // _2 service actor
  var jobVars: HashMap[Int, (ReduceFunction, ActorRef)] = HashMap.empty

  override def receive: Receive = {
    case MapOperationEmit(jobID, key, results) =>

      /*
      log.info("[" + akka.serialization.Serialization.serializedActorPath(self) + "] received MapOpEmit#" + jobID +
      "from " + sender().path.name)
      */

      val reduceOp = jobVars.get(jobID).get._1
      reduceOp.reduce(key, results)

    // no more mappings will be received for this job; send the service the results so far
    case MapOperationComplete(jobID) =>

      //log.info("[" + akka.serialization.Serialization.serializedActorPath(self) + "] received MapOpComplete#" + jobID)

      val serviceActor = jobVars.get(jobID).get._2
      serviceActor ! ReduceOperationEmit(jobID, jobVars.get(jobID).get._1.get())

      log.info("[" + akka.serialization.Serialization.serializedActorPath(self) + "] sent ReduceOpEmit#" + jobID + " for " +
        jobVars.get(jobID).get._1.get().size + " recs")


    case ReduceOperation(jobID, reduceFuncFactory) =>

      //log.info("[" + akka.serialization.Serialization.serializedActorPath(self) + "] received ReduceOperation#" + jobID)

      val serviceActor = sender()
      val thisJobVars = (reduceFuncFactory.getNew(), serviceActor)
      jobVars = jobVars.+ ((jobID, thisJobVars))

      serviceActor ! ReduceOperationAck(jobID, self)
  }
}
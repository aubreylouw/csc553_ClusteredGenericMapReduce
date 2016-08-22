package clusteredmapreduce

import akka.cluster.ClusterEvent.{InitialStateAsEvents, MemberEvent, MemberUp, UnreachableMember}
import akka.actor.{Actor, ActorLogging, ActorPath, ActorRef, Address, RootActorPath}
import scala.collection.immutable.HashMap
import akka.cluster.Cluster
import scala.util.Random
import messages._

class MapReduceServiceReceptionist extends Actor with ActorLogging {
  import context.dispatcher

  // the map reduce service to be discovered by listening to cluster events
  val cluster = Cluster(context.system)
  var serviceSystem:Option[Address] = None
  var nodes:Set[Address] = Set()

  // store client actorref requesting a job
  var clientJobsRegister: HashMap[Int, ActorRef] = HashMap.empty

  // subscribe to cluster changes, re-subscribe when restart
  override def preStart(): Unit = {
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
      classOf[MemberEvent], classOf[UnreachableMember])
  }

  override def postStop(): Unit = cluster.unsubscribe(self)

  override def receive: Receive = {
    case MemberUp(m) => {
      nodes += m.address
      log.info(self.path.name + " knows about " + m.address)

      if (m.hasRole("service")) serviceSystem = Some(m.address)
    }

    case MapReduceJobComplete(jobID, results) => {
      val numRecords = results.size
      log.info("[" + akka.serialization.Serialization.serializedActorPath(self) + "]" +
      s" sending notification to client that $jobID completed successfull with $numRecords records")
      val client: ActorRef = clientJobsRegister.get(jobID).get
      client ! MapReduceJobComplete(jobID, results)
    }

    case MapReduceJobFailure(jobID, reason) => {
      val client: ActorRef = clientJobsRegister.get(jobID).get
      log.info("[" + akka.serialization.Serialization.serializedActorPath(self) + s"] logged a failure for $jobID due to $reason; updating client $client")
      client ! MapReduceJobFailure(jobID, reason)
    }

    case MapReduceJobRequest(resourceSeq, dataFetchFuncFactory, mapFuncFactory, reduceFuncFactory) => {
      val jobID = getJobID
      val job  = MapReduceJob(jobID,
        resourceSeq, dataFetchFuncFactory, mapFuncFactory, reduceFuncFactory)
      clientJobsRegister = clientJobsRegister.+((jobID, sender()))

      log.info("[" + akka.serialization.Serialization.serializedActorPath(self) + s"] received request $jobID")

      // submit a job to the map reduce service
      val serviceSystemPath: ActorPath = RootActorPath(serviceSystem.get) / "user" / "service"
      context.system.actorSelection(serviceSystemPath) ! job

      // send the client a jobID
      sender() ! MapReduceJobConfirmation(jobID)
    }
  }

  def getJobID(): Int = {
    new Random(Integer.MAX_VALUE).nextInt()
  }
}
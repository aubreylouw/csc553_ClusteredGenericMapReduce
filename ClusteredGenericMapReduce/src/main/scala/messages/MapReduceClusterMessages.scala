package messages

import akka.actor.ActorRef

abstract class DataFetchFunction extends Serializable {
  def fetch(resource:String): String
}

abstract class MapFunction extends Serializable{
  def map(key:String, values:String): Unit
  def get(): List[(String,String)]
}

abstract class ReduceFunction extends Serializable{
  def reduce(key:String, values:List[(String,String)]): Unit
  def get(): List[String]
}

abstract class DataFetchFuncFactory extends Serializable {
  def getNew(): DataFetchFunction
}
abstract class MapFuncFactory extends Serializable {
  def getNew(): MapFunction
}
abstract class ReduceFuncFactory extends Serializable {
  def getNew(): ReduceFunction
}

trait ClusterMessages extends Serializable
//**********
// client sends a map reduce job requyest to a client
case class MapReduceJobRequest(resourceSeq:Seq[String],
                               dataFetchFuncFactory: DataFetchFuncFactory,
                               mapFuncFactory: MapFuncFactory,
                               reduceFuncFactory: ReduceFuncFactory) extends ClusterMessages
// receptionist sends an acknowledgment to the client
case class MapReduceJobConfirmation(jobID:Int) extends ClusterMessages
//**********
// receptionist sends the map service a job
// map service will send a mapoperation for each resource in the mapreducejob resourceSeq to a set of map actors
case class MapReduceJob(jobID:Int, resourceSeq:Seq[String],
                        dataFetchFuncFactory: DataFetchFuncFactory,
                        mapFuncFactory: MapFuncFactory,
                        reduceFuncFactory: ReduceFuncFactory) extends ClusterMessages
// map service will send the receptionist a success or failure message
case class MapReduceJobComplete(jobID:Int, result:List[String]) extends ClusterMessages
case class MapReduceJobFailure(jobID:Int, reason:String) extends ClusterMessages

//**********
// each map actor will
// (a) fetch the data using the DataFetchFunction,
// (b) apply the mapFunction,
// (c) send the results back to the service (which forwards it to reducers)
case class MapOperation(jobID:Int, resource:String, dataFetcher: DataFetchFunction,
                          mapFunction: MapFunction) extends ClusterMessages
// multiple map operation failures will result in a mapping failure to be handled by the service
case class MapOperationComplete(jobID:Int) extends ClusterMessages
case class MapOperationFailure(jobID:Int, reason:String) extends ClusterMessages
// a reduce actor will reduce the results using that id's reduce function [see message reduceoperation]
case class MapOperationEmit(jobID:Int, key: String, emit: List[(String, String)]) extends ClusterMessages

//**********
// map service will forward a ReduceOperation to the reducer to use for a particular jobID
case class ReduceOperation(jobID:Int, reduceFuncFactory: ReduceFuncFactory) extends ClusterMessages
//reducer will acknowledge the service so that it knows how many routers received this message
case class ReduceOperationAck(jobID:Int, actorRef: ActorRef) extends ClusterMessages
// reducer actor will send the reduction result back to the service
case class ReduceOperationEmit(jobID:Int, result:List[String]) extends ClusterMessages


case class ACK(jobID:Int) extends ClusterMessages

//generic functions
//case class MapFunction[K<:Any, V<:Any, K2<:Any, V2<:Any](func:((K, V)=>List[(K2,V2)])) extends Serializable
//case class ReduceFunction[K<:Any, V2<:Any](func:((K, List[V2])=>List[V2])) extends Serializable
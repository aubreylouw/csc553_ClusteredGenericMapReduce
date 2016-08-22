package clusteredmapreduce

import akka.actor.{Actor, ActorLogging}

class MapReduceWorker extends Actor with ActorLogging {

  println("MapReduceWorker constructor")

  override def receive: Receive = {
    case msg:String => println("[" + self.path.address + "] " + msg)
  }
}
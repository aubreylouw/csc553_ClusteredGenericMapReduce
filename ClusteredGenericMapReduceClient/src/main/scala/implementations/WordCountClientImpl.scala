package implementations

import messages._

import scala.collection.immutable.HashMap
import scala.io.Source

object wordCountDataFetchFuncFactory extends DataFetchFuncFactory{
  override def getNew(): DataFetchFunction = new wordCountDataFetchFunc()
}

object wordCountMapFuncFactory extends MapFuncFactory {
  def getNew(): MapFunction = new wordCountMapFunc()
}

object wordCountReduceFuncFactory extends ReduceFuncFactory {
  def getNew(): ReduceFunction = new wordCountReduceFunc()
}

class wordCountDataFetchFunc extends DataFetchFunction {

  override def fetch(resource: String): String = Source.fromFile(resource).mkString
}

class wordCountMapFunc extends MapFunction {

  var words: List[(String, String)] = List.empty

  override def map(key: String, values: String): Unit = {
    val candidateWords = values.split("\\W").
              map(_.trim).
              filter(_.size>0).
              map(_.toUpperCase)
    candidateWords.foreach (word => words = words.::(word, "1"))
  }

  override def get(): List[(String,String)] = words
}

class wordCountReduceFunc extends ReduceFunction {
  var words: HashMap[String, Int] =  HashMap.empty
  override def reduce(key: String, values: List[(String,String)]): Unit = {
    values.foreach(value => {
        val key: String = value._1
        var count: Int = value._2.toInt

        if (words.contains(key)) count += words.get(key).get

        words = words +((key, count))
      }
    )
  }

  override def get(): List[String] = {
    var results:List[String] =List.empty
    val iter: Iterator[(String, Int)] = words.toIterator

    while (iter.hasNext) {
      val keyValue: (String, Int) = iter.next()
      results = results.::(new String(keyValue._1 + "-->" + keyValue._2.toString))
    }
    results
  }
}
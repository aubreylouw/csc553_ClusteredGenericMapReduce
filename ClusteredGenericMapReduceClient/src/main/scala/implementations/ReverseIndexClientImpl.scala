package implementations

import messages._

import scala.collection.immutable.HashMap
import scala.io.Source

object reverseIndexDataFetchFuncFactory extends DataFetchFuncFactory{
  override def getNew(): DataFetchFunction = new reverseIndexDataFetchFunc()
}

object reverseIndexMapFuncFactory extends MapFuncFactory {
  def getNew(): MapFunction = new reverseIndexMapFunc()
}

object reverseIndexReduceFuncFactory extends ReduceFuncFactory {
  def getNew(): ReduceFunction = new reverseIndexReduceFunc()
}

class reverseIndexDataFetchFunc extends DataFetchFunction {

  override def fetch(resource: String): String = Source.fromFile(resource).mkString
}

class reverseIndexMapFunc extends MapFunction {

  var words: List[(String, String)] = List.empty

  override def map(key: String, values: String): Unit = {
    val candidateWords = values.split("\\W").
      map(_.trim).
      filter(_.size>0).
      map(_.toUpperCase)
    candidateWords.foreach (word => words = words.::(word, key))
  }

  override def get(): List[(String,String)] = words
}

class reverseIndexReduceFunc extends ReduceFunction {
  var indexedWords: HashMap[String, Set[String]] =  HashMap.empty
  override def reduce(key: String, values: List[(String,String)]): Unit = {
    values.foreach(value => {
      val word: String = value._1
      var sources: Set[String] = Set(value._2)

      if (indexedWords.contains(key)) sources = sources.++(indexedWords.get(word).get)

      indexedWords = indexedWords +((key, sources))
    }
    )
  }

  override def get(): List[String] = {
    var results:List[String] =List.empty
    val iter: Iterator[(String, Set[String])] = indexedWords.toIterator

    while (iter.hasNext) {
      val keyValue: (String, Set[String]) = iter.next()
      results = results.::(new String(keyValue._1 + "-->" + keyValue._2.toString))
    }
    results
  }
}
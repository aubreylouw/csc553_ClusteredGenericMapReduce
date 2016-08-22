package implementations

import scala.collection.immutable.HashMap
import scala.io.Source
import java.io.{File, InputStream}
import messages._

object countIncidentEdgesDataFetchFuncFactory extends DataFetchFuncFactory{
  override def getNew(): DataFetchFunction = new countIncidentEdgesDataFetchFunc()
}

object countIncidentEdgesMapFuncFactory extends MapFuncFactory {
  def getNew(): MapFunction = new countIncidentEdgesMapFunc()
}

object countIncidentEdgesReduceFuncFactory extends ReduceFuncFactory {
  def getNew(): ReduceFunction = new countIncidentEdgesReduceFunc()
}

class countIncidentEdgesDataFetchFunc extends DataFetchFunction {

  override def fetch(resource: String): String = {

    var result:List[String] = List.empty
    val lines = Source.fromFile(resource).getLines
    val iter:Iterator[String] = Source.fromFile(resource).getLines()
    while (iter.hasNext) {
      val value = iter.next()
      if (value.startsWith("<a href=")) {
        val start = value.indexOf("\"",0)
        val end = value.indexOf("\"",start+1)
        val link = value.substring(start, end)
        result = result.::(link)
      }
    }
    result.mkString(";")
  }
}

class countIncidentEdgesMapFunc extends MapFunction {

  var links: List[(String, String)] = List.empty

  override def map(key: String, values: String): Unit = {
    val iter:Iterator[String] = values.split(";").iterator
    while (iter.hasNext) {
      links = links.::((iter.next(), "1"))
    }
  }

  override def get(): List[(String,String)] = links
}

class countIncidentEdgesReduceFunc extends ReduceFunction {
  var incidentEdges: HashMap[String, Int] =  HashMap.empty
  override def reduce(key: String, values: List[(String,String)]): Unit = {
    values.foreach(value => {
      val key: String = value._1
      var count: Int = value._2.toInt

      if (incidentEdges.contains(key)) count += incidentEdges.get(key).get

      incidentEdges = incidentEdges+((key, count))
    }
    )
  }

  override def get(): List[String] = {
    var results:List[String] =List.empty
    val iter: Iterator[(String, Int)] = incidentEdges.toIterator

    while (iter.hasNext) {
      val keyValue: (String, Int) = iter.next()
      results = results.::(new String(keyValue._1 + "-->" + keyValue._2.toString))
    }
    results
  }
}
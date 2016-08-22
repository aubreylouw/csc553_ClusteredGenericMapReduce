package mapreduceclient

object clientImplementations {
  sealed trait clientImplementationsTrait
  case class WORD_COUNT(files:Seq[String]) extends clientImplementationsTrait
  case class REVERSE_INDEX(files:Seq[String]) extends clientImplementationsTrait
  case class PAGERANK(files:Seq[String]) extends clientImplementationsTrait

  def getImplementation(args:String, files: Seq[String]): Option[clientImplementationsTrait] = {
    var result: Option[clientImplementationsTrait] = Option(null)
    if (args.equals("word_count")) result = Some(WORD_COUNT(files:Seq[String]))
    else if (args.equals("reverse_index")) result = Some(REVERSE_INDEX(files:Seq[String]))
    else if (args.equals("pagerank")) result = Some(PAGERANK(files:Seq[String]))
    result
  }
}
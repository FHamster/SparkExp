object PropertiesObj {
  val pwd: String = System.getProperty("user.dir")
  val wholeDBLP = "/whole/dblp.xml"
  val wholeDBLP_cvt = "/whole/dblp_cvt.xml"
  lazy val wholeDBLP_cvtSparkPath: String = s"file://${pwd + wholeDBLP_cvt}"
  lazy val wholeDBLP_SparkPath: String = s"file://${pwd + wholeDBLP}"

  lazy val subNode = Seq(
    "article",
    "inproceedings",
    "proceedings",
    "book",
    "incollection",
    "phdthesis",
    "mastersthesis",
    "www",
    "person",
    "data"
  )
}

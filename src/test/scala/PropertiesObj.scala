import org.apache.spark.sql.types._

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

  //手工设定的article schema
  //虽然nullable参数默认是true，但是还是写着
  //我只是喜欢下面这种方式构造schema。其实也可以使用StructType.add的方式添加StructField
  val ManualArticleSchema = new StructType(Array(
    StructField("_cdate", StringType, nullable = true),
    StructField("_key", StringType, nullable = true),
    StructField("_mdate", StringType, nullable = true),
    StructField("_publtype", StringType, nullable = true),
    StructField("author", ArrayType(
      StructType(Array(
        StructField("_VALUE", StringType, nullable = true),
        StructField("_orcid", StringType, nullable = true),
        StructField("_aux", StringType, nullable = true)
      )), containsNull = true)
    ),
    StructField("title", StringType, nullable = true),
    StructField("url", StringType, nullable = true),
    StructField("cdrom", StringType, nullable = true),
    StructField("ee", StringType, nullable = true),
    StructField("journal", StringType, nullable = true),
    StructField("month", StringType, nullable = true),
    StructField("year", LongType, nullable = true),
    StructField("note", StringType, nullable = true),
    StructField("publisher", StringType, nullable = true),
    StructField("volume", StringType, nullable = true)
  ))
}

import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.SparkSession


/**
 * 这个类记录了如何将spark的数据写入mongodb
 */
object MongoSparkTest {
  val testFile = "src/test/resources/article_after.xml"
  val testFile2 = "src/test/resources/article_CharTest.xml"

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("XML_WriteTest")
      .master("local[*]")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/scalaTest.test3")
      .getOrCreate()

    import com.databricks.spark.xml._
    val opt = spark.read
      .option("rootTag", "dblp")
      .option("rowTag", "article")
      .option("charset", "UTF-8")
      .xml(testFile2)

    opt.printSchema()
    opt.show()

    MongoSpark.save(opt)
  }
} //[Fahad Khan,null,0000-0002-1551-7438]

//<author orcid="0000-0002-1551-7438">Fahad Khan</author>
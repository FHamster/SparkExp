import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite


/**
 * 这个类记录了如何将spark的数据写入mongodb
 */
class MongoSparkTest extends AnyFunSuite {
  val testFile = "src/test/resources/article_after.xml"
  val testFile2 = "src/test/resources/article_CharTest.xml"
  val spark = SparkSession.builder
    .appName("XML_WriteTest")
    .master("local[*]")
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/scalaTest.test3")
    .getOrCreate()
  test("write into mongodb") {
    import com.databricks.spark.xml._
    val opt = spark.read
      .option("rootTag", "dblp")
      .option("rowTag", "article")
      .option("charset", "UTF-8")
      .xml(testFile2)

    opt.show()
    MongoSpark.save(opt)
  }
}
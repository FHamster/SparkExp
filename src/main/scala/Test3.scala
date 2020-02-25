import org.apache.spark.sql.SparkSession
import com.mongodb.spark._
import com.mongodb.spark.config._
import org.bson.Document

object Test3 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("XML_Test")
      .master("local[*]")
      //      .config("driver-memory", "4096M")
//      .config("spark.executor.memory", "4G")
      .config("spark.mongodb.output.uri","mongodb://127.0.0.1/scalaTest.test")
      .getOrCreate()


    import  spark.implicits._

    //    val df = spark.read.format("com.databricks.spark.xml").option("rowTag", "article").load("file:///root/dblp.xml")
    //    val df = spark.read.format("com.databricks.spark.xml").option("rootTag", "dblp").option("rowTag", "article").load("file:///root/dblp.xml")
    val df = spark.read
      .format("com.databricks.spark.xml")
      .option("rootTag", "dblp")
      .option("rowTag", "article")
      .option("charset", "ISO-8859-1")
      .load("file:////Users/linmouhan/IdeaProjects/SparkExp/article.xml").toDF()

    //LogExample.setStreamingLogLevels()

    //    df.select("_corrupt_record").foreach(row => println(row))
    //          .load("dblp.xml")
    //      .load("a.xml")

    //    val df = spark.read.option("rowTag", "dblp").load("dblp.xml")

    //    val selectedData = df.select("author", "_id")

    //df.show(100)

    //val test = spark.sparkContext.parallelize(Seq(df))

    //println(test)

    MongoSpark.save(df)

  }
}

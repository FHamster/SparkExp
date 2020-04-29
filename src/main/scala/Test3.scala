import ReplaceTagUtil.atorRegex
import org.apache.spark.sql.SparkSession
import com.mongodb.spark._
import com.mongodb.spark.config._
import org.bson.Document

import scala.collection.mutable
import scala.util.matching.Regex


object Test3 {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("XML_Test")
      .master("local[*]")
      //      .config("driver-memory", "4096M")
//      .config("spark.executor.memory", "4G")
      //.config("spark.mongodb.output.uri","mongodb://127.0.0.1/scalaTest.after")
      //.config("spark.mongodb.input.uri","mongodb://127.0.0.1/scalaTest.after")
      .getOrCreate()

    import  spark.implicits._

    //val df = spark.read.format("com.databricks.spark.xml").option("rowTag", "article").load("file:///root/dblp.xml")
    //    val df = spark.read.format("com.databricks.spark.xml").option("rootTag", "dblp").option("rowTag", "article").load("file:///root/dblp.xml")
    val df = spark.read
      .format("com.databricks.spark.xml")
      .option("rootTag", "dblp")
      .option("rowTag", "article")
      .option("charset", "UTF-8")
      .load("file:////Users/linmouhan/IdeaProjects/SparkExp/article_after.xml").toDF()

    //val df = MongoSpark.load(spark).toDF()

    //LogExample.setStreamingLogLevels()

    //    df.select("_corrupt_record").foreach(row => println(row))
    //          .load("dblp.xml")
    //      .load("a.xml")

    //    val df = spark.read.option("rowTag", "dblp").load("dblp.xml")

    //    val selectedData = df.select("author", "_id")



//    var nameStr = df.select($"author"("_VALUE").alias("author"))
//      //.dropDuplicates
//      .collect()
//      .mkString
//      .toString
//
//    var newstr =ReplaceWrapped.wpParse(nameStr).split(",").map(x => x.trim).toBuffer
//
//    var authorDf = newstr.toDF("author")
//      .dropDuplicates
//      .show(100)
//
    df.select($"author"("_VALUE"),$"_key",$"title").show(100)






//      .write.format("com.databricks.spark.xml")
//      .save("file:////Users/linmouhan/IdeaProjects/SparkExp/output/out1")
//      //.show()


    //val test = spark.sparkContext.parallelize(Seq(df))

    //println(test)

    //MongoSpark.save(df)

    //df.rdd.saveAsTextFile("file:////Users/linmouhan/IdeaProjects/SparkExp/text1.xml")
//    val testText2 = "<title>The Many-Valued <i> sd </i> Theorem Prover<sub>3</sub><sub>3</sub>T<sup>A</sup>P. (i) i (/i)</title>";
//
//    val testText = "<i>";
//    //def main(args: Array[String]): Unit = {
//
//      val text = ReplaceTagUtil.atorParse(testText2);
//      printf(text);
//      println("\n");
//      printf(ReplaceTagUtil.rtoaAarse(text));


    //}

  }
}

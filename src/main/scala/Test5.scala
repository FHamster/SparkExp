import org.apache.spark.sql.SparkSession

object Test5 {
  val s1 = "file:///root/dblp.xml"
  val s2 = "file:///root/dblp_after.xml"
  val s3 = "file:////Users/gaoxin/WorkSpace/spark/article_after.xml"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("XML_WriteTest")
      .master("local[*]")
      .getOrCreate()


    val opt = spark.read.format("com.databricks.spark.xml")
      .option("rootTag", "dblp")
      .option("rowTag", "article")
      .load("article_after.xml")

    opt.printSchema()
    opt.show()
    opt.select("author").take(100).foreach(println(_))
//opt.filter(_.)
    opt.write.format("com.databricks.spark.xml")
      .option("path","article_after2.xml")
      .option("rootTag","dblp")
      .option("rowTag", "article")
      .save()


    opt.show()
  }
}//[Fahad Khan,null,0000-0002-1551-7438]

//<author orcid="0000-0002-1551-7438">Fahad Khan</author>
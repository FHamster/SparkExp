import java.io.File
import org.apache.spark.sql.SparkSession

/**
 * 更改实体定义
 */
object ConversionEntity extends App {
  val s1 = "file:///root/dblp.xml"
  val s2 = "file:///root/dblp_after.xml"
  val s3 = "file:////Users/gaoxin/WorkSpace/spark/article.xml"
  val s4 = "file:////Users/gaoxin/WorkSpace/spark/article_after.xml"

  val spark = SparkSession
    .builder
    .appName("XML_Test")
    .master("local[*]")
    .getOrCreate()

  val subNode = Array("article")

  //读取
  val file = spark.sparkContext.textFile(s1)

  //转换实体
  val afterParse = file.map(s => ReplaceEntityUtil.parse(s)) //.cache()

  //afterParse.foreach(it => println(it))

  val afterParedblp: File = new File(s2)

  if (afterParedblp.isFile) {
    println("delete " + afterParedblp.delete())
  }

  afterParse.saveAsTextFile(s2)
}
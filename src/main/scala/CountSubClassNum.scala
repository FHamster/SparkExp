import org.apache.spark.sql.SparkSession

/*
(article,2093906)
(inproceedings,2450274)
(proceedings,41782)
(book,17714)
(incollection,59477)
(phdthesis,73252)
(mastersthesis,12)
(www,2346866)
(person,0)
(data,0)
 */
//统计dblp下的二级分类记录的数量
object CountSubClassNum extends App {
  val spark = SparkSession
    .builder
    .appName("XML_Test")
    .master("local[*]")
    .getOrCreate()

  val subNode = Array("article", "inproceedings", "proceedings", "book",
    "incollection", "phdthesis", "mastersthesis", "www", "person", "data")

  val subNodeRowNum = subNode.map(it => {
    val rowNum: Long = spark.read
      .format("com.databricks.spark.xml")
      .option("rootTag", "dblp")
      .option("rowTag", it)
      .load("file:///root/dblp.xml").count()
    (it, rowNum)
  })
    .array

  subNodeRowNum.foreach(it => println(it))
  spark.stop()
}

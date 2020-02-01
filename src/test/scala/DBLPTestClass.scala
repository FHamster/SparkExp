import java.nio.file.Path

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import scala.collection.mutable.Map

//做的一些测试
final class DBLPTestClass extends FunSuite with BeforeAndAfterAll {
  val testRes: String = "src/test/resources/article.xml"
  private lazy val spark: SparkSession = {
    // It is intentionally a val to allow import implicits.
    SparkSession.builder()
      .master("local[*]")
      .appName("DBLPTest")
      .config("spark.ui.enabled", value = false)
      .getOrCreate()
  }

  private lazy val dblpArticle: DataFrame = {
    import com.databricks.spark.xml._
    spark.read
      .option("rootTag", "dblp")
      .option("rowTag", "article")
      .xml(testRes)
      .cache()
  }
  private var tempDir: Path = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    //    spark // Initialize Spark session
    //    tempDir = Files.createTempDirectory("DBLPTestDir")
    //    tempDir.toFile.deleteOnExit()
  }


  override protected def afterAll(): Unit = {
    try {
      spark.stop()
    } finally {
      super.afterAll()
    }
  }

  test("show article dataframe") {
    dblpArticle.printSchema()
    dblpArticle.show()

    //    dblpArticle.
  }
  test("Select some column from article") {
    import spark.implicits._
    dblpArticle.select($"title", $"author", $"url")
      .show()
  }
  test("filter with number of long type") {
    import spark.implicits._
    dblpArticle.select($"title", $"author", $"year").filter($"year" > 2000)
      .show()
  }
  test("filter with exact string match") {
    import spark.implicits._
    dblpArticle.filter($"title" <=> "Knowledge in Operation")
      .show()
  }
  test("filter with sql like") {
    import spark.implicits._
    dblpArticle.filter($"title" like "%Knowledge%")
      .show()
  }

  test("filter with regex string") {
    import spark.implicits._
    dblpArticle.select($"title", $"author", $"url").filter($"title" rlike "^Knowledge").show()
  }

  test("filter with complex struct") {
    import spark.implicits._

    dblpArticle.filter(array_contains($"author._VALUE", "Paul Kocher"))
      .show()
  }

  test("export into array") {
    import spark.implicits._

    val cache: DataFrame = dblpArticle
      .filter(array_contains($"author._VALUE", "Paul Kocher"))
      .cache()

    cache.printSchema()
    val a: Array[Row] = cache.take(1)

    //    println(a)

    //    a(0).getList()
    a(0).schema.fieldNames.foreach(println(_));


    println()
    println(a(0).getAs(fieldName = "_key"));

    println(a(0).getAs(fieldName = "author"));
  }

  test("read dataframe schma") {
    import spark.implicits._

    val cache: DataFrame = dblpArticle
      .filter(array_contains($"author._VALUE", "Paul Kocher"))
      .cache()

    cache.printSchema()
    val a: Array[Row] = cache.take(1)

    //    println(a)

    //    a(0).getList()
    //    a(0).schema.fieldNames.foreach(println(_));

    //    a(0).schema.printTreeString()

    //    a(0).schema.fields.foreach(println(_))
    val fields: Array[StructField] = a(0).schema.fields
    var map: Map[String, Any] = Map()
    fields.foreach((field: StructField) => {
      println(s"${field.name} = ${a(0).getAs(fieldName = field.name)} ")
      map += (field.name -> a(0).getAs(fieldName = field.name))
    })

    println(map)
    println(map)

  }


  test("filter with xpath") {

    //xpath函数是sparksql的内置函数
    //由于需要把xml字符串作为入参传进去，觉得不太好用
    /*
    +-----------------------------------------------------------------------+
    |xpath(<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>, a/b/text())|
    +-----------------------------------------------------------------------+
    |                                                           [b1, b2, b3]|
    +-----------------------------------------------------------------------+
     */
    spark.sqlContext.sql("""SELECT xpath('<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>','a/b/text()')""").show()
  }
  test("read dblp by string") {
    //我尝试将数据源以字符串的形式读入，再转换为scala的xml字面量做处理
    //这需要进行一些处理，因为spark读取字符串是以行为单位读取的。
    //spark-xml似乎是用stax做的

    val a: xml.Elem =
      <article mdate="2018-01-07" key="tr/meltdown/s18" publtype="informal">
        <author>Paul Kocher</author>
        <author>Daniel Genkin</author>
        <title>Spectre Attacks: Exploiting Speculative Execution.</title>
        <journal>meltdownattack.com</journal>
        <year>2018</year>
        <ee>https://spectreattack.com/spectre.pdf</ee>
      </article>

    val s = a \ "author"
    println(s)

  }
}
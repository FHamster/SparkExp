import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.shell.CopyCommands.Put
import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.client.Table
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.BasicConfigurator
import org.apache.zookeeper.Op.Delete
import scala.reflect.internal.pickling.UnPickler.Scan
import java.io.BufferedReader
import java.io.IOException
import java.io.InputStreamReader
import java.util
import java.util.Scanner


object HbaseTest {
  var admin: Nothing = null
  var conf: Configuration = null
  var connection: Nothing = null
  var dis = "hdfs://106.13.6.196:9000/hbase"

  /**
   * connect hbase
   */
  def init(): Unit = {
    conf = HBaseConfiguration.create
    //conf.set("hbase.rootdir",dis);
    conf.set("hbase.zookeeper.quorum", "106.13.6.196")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    try {
      connection = ConnectionFactory.createConnection(conf)
      admin = connection.getAdmin
    } catch {
      case e: IOException =>
        e.printStackTrace()
    }
  }

  /**
   * close connect
   */
  def close(): Unit = {
    try {
      if (admin != null) admin.close
      if (connection != null) connection.close
    } catch {
      case e: IOException =>
        e.printStackTrace()
    }
  }

  /**
   * 创建表
   *
   * @param tableName
   * @param fields
   * @throws IOException
   */
  @throws[IOException]
  def createTable(tableName: String, fields: Array[String]): Unit = {
    init()
    val tablename = TableName.valueOf(tableName)
    if (admin.tableExists(tablename)) {
      System.out.println("table is exists")
      admin.disableTable(tablename)
      admin.deleteTable(tablename)
    }
    val tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName))
    for (str <- fields) {
      val columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(str)).build
      tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor)
    }
    admin.createTable(tableDescriptorBuilder.build)
    close()
  }

  /**
   * 添加数据
   *
   * @param tableName
   * @param row
   * @param fields
   * @param values
   * @throws IOException
   */
  @throws[IOException]
  def addRecord(tableName: String, row: String, fields: Array[String], values: Array[String]): Unit = {
    init()
    val table = connection.getTable(TableName.valueOf(tableName))
    val put = new CopyCommands.Put(row.getBytes)
    for (i <- 0 until fields.length) {
      val col = fields(i).split(":")
      put.addColumn(col(0).getBytes, col(1).getBytes, values(i).getBytes)
    }
    table.put(put)
    table.close
    close()
  }

  /**
   * 显示单列数据
   *
   * @param tableName
   * @param column
   * @throws IOException
   */
  @throws[IOException]
  def scanColumn(tableName: String, column: String): Unit = {
    init()
    val table = connection.getTable(TableName.valueOf(tableName))
    val sc = new UnPickler#Scan
    sc.addFamily(column.getBytes)
    val resultScanner = connection.getTable(TableName.valueOf(tableName)).getScanner(sc)
    var result = resultScanner.next
    while ( {
      result != null
    }) {
      showCell(result)

      result = resultScanner.next
    }
    table.close
    close()
  }

  /**
   * 修改数据
   *
   * @param tableName
   * @param row
   * @param column
   * @param val
   * @throws IOException
   */
  @throws[IOException]
  def modifyData(tableName: String, row: String, column: String, `val`: String): Unit = {
    init()
    val table = connection.getTable(TableName.valueOf(tableName))
    val put = new CopyCommands.Put(row.getBytes)
    val cols = column.split(":")
    if (cols.length == 1) put.addColumn(column.getBytes, "".getBytes, `val`.getBytes)
    else put.addColumn(cols(0).getBytes, cols(1).getBytes, `val`.getBytes)
    table.put(put)
    table.close
    close()
  }

  /**
   * 删除表
   *
   * @param tableName
   * @param row
   * @throws IOException
   */
  @throws[IOException]
  def deleteRow(tableName: String, row: String): Unit = {
    try {
      init()
      val table = connection.getTable(TableName.valueOf(tableName))
      val delete = new Op.Delete(Bytes.toBytes(row))
      table.delete(delete)
      table.close
    } catch {
      case e: IOException =>
        e.printStackTrace()
    }
    close()
  }

  def showCell(result: Nothing): Unit = {
    val cells = result.rawCells
    for (cell <- cells) {
      System.out.println("RowName:" + new String(CellUtil.cloneRow(cell)) + " ")
      System.out.println("Timetamp:" + cell.getTimestamp + " ")
      System.out.println("column Family:" + new String(CellUtil.cloneFamily(cell)) + " ")
      System.out.println("row Name:" + new String(CellUtil.cloneQualifier(cell)) + " ")
      System.out.println("value:" + new String(CellUtil.cloneValue(cell)) + " ")
    }
  }

  @throws[IOException]
  def getData(tableName: String): Unit = {
    init()
    val table = connection.getTable(TableName.valueOf(tableName))
    val scan = new UnPickler#Scan
    val scanner = table.getScanner(scan)
    import scala.collection.JavaConversions._
    for (result <- scanner) {
      showCell(result)
    }
    close()
  }

  @throws[IOException]
  def main(args: Array[String]): Unit = { // TODO Auto-generated method stub
    val hbaseTest = new HbaseTest
    BasicConfigurator.configure()
    var flag = true
    while ( {
      flag
    }) {
      System.out.println("------------------------------------------------提供以下功能----------------------------------------------")
      System.out.println("                       1- createTable（创建表  ,提供表名、列族名）                                      ")
      System.out.println("                       2- addRecord （向已知表名、行键、列簇的表添加值）                       ")
      System.out.println("                       3- ScanColumn（浏览表     某一列的数据）                                            ")
      System.out.println("                       4- modifyData（修改某表   某行，某一列，指定的单元格的数据）    ")
      System.out.println("                       5- deleteRow（删除 某表   某行的记录）                                                 ")
      System.out.println("------------------------------------------------------------------------------------------------------------------")
      var scan = new Scanner(System.in)
      val choose1 = scan.nextLine.toInt
      choose1 match {
        case 1 =>
          System.out.println("请输入要创建的表名")
          val tableName = scan.nextLine
          System.out.println("请输入要创建的表的列族个数")
          val Num = scan.nextInt
          val fields = new Array[String](Num)
          System.out.println("请输入要创建的表的列族")
          /* Scanner scanner = new Scanner(System.in);     scanner.next 如不是全局，即会记得上一次输出。相同地址读入值时*/ for (i <- 0 until fields.length) {
          /*fields[i]=scan.next(); 因为之前没有输入过，所以可以读入新值*/ scan = new Scanner(System.in)
          fields(i) = scan.nextLine
        }
          System.out.println("正在执行创建表的操作")
          hbaseTest.createTable(tableName, fields)


        case 2 =>
          System.out.println("请输入要添加数据的表名")
          val tableName = scan.nextLine
          System.out.println("请输入要添加数据的表的行键")
          val rowKey = scan.nextLine
          System.out.println("请输入要添加数据的表的列的个数")
          val num = scan.nextInt
          val fields = new Array[String](num)
          System.out.println("请输入要添加数据的表的列信息 共" + num + "条信息")
          for (i <- 0 until fields.length) {
            val in3 = new BufferedReader(new InputStreamReader(System.in))
            fields(i) = in3.readLine
          }
          System.out.println("请输入要添加的数据信息 共" + num + "条信息")
          val values = new Array[String](num)
          for (i <- 0 until values.length) {
            val in2 = new BufferedReader(new InputStreamReader(System.in))
            values(i) = in2.readLine
          }
          System.out.println("原表信息如下：........\n")
          hbaseTest.getData(tableName)
          System.out.println("正在执行向表中添加数据的操作........\n")
          hbaseTest.addRecord(tableName, rowKey, fields, values)
          System.out.println("\n添加后的表的信息........")
          hbaseTest.getData(tableName)


        case 3 =>
          System.out.println("请输入要查看数据的表名")
          val tableName = scan.nextLine
          System.out.println("请输入要查看数据的列名")
          val column = scan.nextLine
          System.out.println("查看的信息如下：........\n")
          hbaseTest.scanColumn(tableName, column)


        case 4 =>
          System.out.println("请输入要修改数据的表名")
          val tableName = scan.nextLine
          System.out.println("请输入要修改数据的表的行键")
          val rowKey = scan.nextLine
          System.out.println("请输入要修改数据的列名")
          val column = scan.nextLine
          System.out.println("请输入要修改的数据信息  ")
          val value = scan.nextLine
          System.out.println("原表信息如下：........\n")
          hbaseTest.getData(tableName)
          System.out.println("正在执行向表中修改数据的操作........\n")
          hbaseTest.modifyData(tableName, rowKey, column, value)
          System.out.println("\n修改后的信息如下：........\n")
          hbaseTest.getData(tableName)


        case 5 =>
          System.out.println("请输入要删除指定行的表名")
          val tableName = scan.nextLine
          System.out.println("请输入要删除指定行的行键")
          val rowKey = scan.nextLine
          System.out.println("原表信息如下：........\n")
          hbaseTest.getData(tableName)
          System.out.println("正在执行向表中删除数据的操作........\n")
          hbaseTest.deleteRow(tableName, rowKey)
          System.out.println("\n删除后的信息如下：........\n")
          hbaseTest.getData(tableName)


        case _ =>
          System.out.println("   你的操作有误 ！！！    ")


      }
      System.out.println(" 你要继续操作吗？ 是-true 否-false ")
      flag = scan.nextBoolean
    }
    System.out.println("   程序已退出！    ")
  }
}

class HbaseTest {}

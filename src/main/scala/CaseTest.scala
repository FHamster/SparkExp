object CaseTest {

  def main(args: Array[String]): Unit = {
    import scala.util.Random
    val randomInt = new Random().nextInt(10)
    val a = randomInt match {
      case 7 => 5
      case 1 => 2
    }

    println(a)
  }
}

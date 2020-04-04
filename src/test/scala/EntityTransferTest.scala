import org.scalatest.{BeforeAndAfterAll, FunSuite}

final class EntityTransferTest extends FunSuite with BeforeAndAfterAll {
  val testText = "<title>&Uuml;ber Ans&auml;tze zur Darstellung von Konzepten und Prototypen</title>"
  test("test transfer character in line") {
    assertResult("<title>&#220;ber Ans&#228;tze zur Darstellung von Konzepten und Prototypen</title>"){
      ReplaceEntityUtil.parse(testText)
    }
  }
}
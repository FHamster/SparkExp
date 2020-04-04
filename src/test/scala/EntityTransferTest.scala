import org.scalatest.funsuite.AnyFunSuite

final class EntityTransferTest extends AnyFunSuite {
  val testText = "<title>&Uuml;ber Ans&auml;tze zur Darstellung von Konzepten und Prototypen</title>"
  test("test transfer character in line") {
    assertResult("<title>&#220;ber Ans&#228;tze zur Darstellung von Konzepten und Prototypen</title>"){
      ReplaceEntityUtil.parse(testText)
    }
  }
}
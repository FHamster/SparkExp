import ReplaceEntityUtil.regex

import scala.util.matching.Regex

object TagUtil {
  private val atorRegex: Regex = new Regex("\\<(sub|i|sup|/i|/sub|/sup)\\>");

  private val rtoaRegex: Regex = new Regex("\\((sub|i|sup|/i|/sub|/sup)\\)");

  def atorParse(s: String): String = atorRegex.replaceAllIn(s, it => it.toString() match{
    case "\\<i\\>" => "\\(i\\)"
    case "\\<sub\\>" => "\\(sub\\)"
    case "\\<sup\\>" => "\\(sup\\)"
    case "\\</i\\>" => "\\(/i\\)"
    case "\\</sub\\>" => "\\(/sub\\)"
    case "\\</sup\\>" => "\\(/sup\\)"
    case _ => {
      println(it)
      it.toString()
    }
  })

  def rtoaAarse(s: String): String = rtoaRegex.replaceAllIn(s, it => it.toString() match{
    case "\\(i\\)" => "\\<i\\>"
    case "\\(sub\\)" => "\\<sub\\>"
    case "\\(sup\\)" => "\\<sup\\>"
    case "\\(/i\\)" => "\\</i\\>"
    case "\\(/sub\\)" => "\\</sub\\>"
    case "\\(/sup\\)" => "\\</sup\\>"
    case _ => {
      println(it)
      it.toString()
    }
  })

}
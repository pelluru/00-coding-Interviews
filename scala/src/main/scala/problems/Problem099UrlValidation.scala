
package problems
import java.util.regex.Pattern

object Problem099UrlValidation {
  private val P = Pattern.compile("^(https?://)?([\\w.-]+)(:[0-9]+)?(/.*)?$")
  def isValid(url:String): Boolean = url!=null && P.matcher(url).matches()
}


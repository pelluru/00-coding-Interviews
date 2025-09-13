
package problems
import java.util.regex.Pattern

object Problem100EmailValidationRegex {
  private val P = Pattern.compile("^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$")
  def isValid(email:String): Boolean = email!=null && P.matcher(email).matches()
}


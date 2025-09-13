package tests

import org.scalatest.funsuite.AnyFunSuite
import problems.Problem072UrlShortenerEncodeDecode

class Problem072UrlShortenerEncodeDecodeSpec extends AnyFunSuite {
  test("shortener") { val c=Problem072UrlShortenerEncodeDecode.encode("http://x"); assert(Problem072UrlShortenerEncodeDecode.decode(c)=="http://x") }
}


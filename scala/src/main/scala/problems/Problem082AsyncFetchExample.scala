package problems
import scala.concurrent._
import scala.concurrent.duration._
import ExecutionContext.Implicits.global
object Problem082AsyncFetchExample {
  def fetchAll(urls: List[String]): Future[List[Int]] = Future.sequence(urls.map(u => Future(u.length)))
}

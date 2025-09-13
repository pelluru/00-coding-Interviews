package problems


object Problem077DecoratorTiming {
  def time[T](f: => T): (T, Long) = {
    val s = System.nanoTime(); val r = f; (r, System.nanoTime()-s)
  }
}


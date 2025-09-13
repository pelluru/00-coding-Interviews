package problems


object Problem078ContextManagerExample {
  def using[A <: AutoCloseable, B](r: A)(f: A => B): B =
    try f(r) finally if (r != null) r.close()
}


package problems


object Problem075DebounceFunction {
  class Debounce(waitMillis:Long) {
    private var last: Long = Long.MinValue
    def run(now:Long)(f: => Unit): Boolean = {
      if (now - last >= waitMillis) { last = now; f; true } else false
    }
  }
}


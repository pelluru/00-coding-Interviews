package problems
object Problem022StackUsingList {
  class StackX[T] { private val s = scala.collection.mutable.ListBuffer[T](); def push(x:T)=s.append(x); def pop():T=s.remove(s.size-1); def peek():T=s.last; def isEmpty:Boolean=s.isEmpty }
}

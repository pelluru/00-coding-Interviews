package problems
object Problem025TopKFrequentWords {
  def topK(words:Array[String], k:Int): List[String] = {
    words.groupBy(identity).view.mapValues(_.length).toList
      .sorted(Ordering.by[(String,Int), (Int,String)]{case (w,c)=>(-c,w)}).take(k).map(_._1)
  }
}

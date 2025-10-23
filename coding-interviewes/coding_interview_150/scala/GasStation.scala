
/* Gas Station — greedy total/tank reset. O(n) */
object GasStation {
  def canCompleteCircuit(gas:Array[Int], cost:Array[Int]):Int = {
    var total=0; var tank=0; var start=0
    for (i <- 0 until gas.length){
      val diff = gas(i) - cost(i)
      total += diff; tank += diff
      if (tank<0){ start=i+1; tank=0 }
    }
    if (total>=0) start else -1
  }
  def main(args:Array[String]):Unit = {
    println(canCompleteCircuit(Array(1,2,3,4,5), Array(3,4,5,1,2)))
  }
}

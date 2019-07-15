package ir.sweetsoft.common

import org.joda.time.DateTime

object SweetOut {

  def printLine(x:String): Unit =
  {
//    println(DateTime.now()+x.toString)
    printLine(x,1)
  }
  def printLine(x:String,Priority:Int): Unit =
  {
    if(Priority>=1)
      println(DateTime.now()+x)
  }
  def printOne(x:Any): Unit =
  {
    print(x.toString)
  }
}

package ir.sweetsoft.common

import org.joda.time.DateTime

object SweetOut {

  def printLine(x:Any): Unit =
  {
    println(DateTime.now()+x.toString)
//    println(x.toString)
  }
  def printLine(x:Any,Priority:Int): Unit =
  {
    if(Priority>=1)
      printLine(x)
  }
  def printOne(x:Any): Unit =
  {
    print(DateTime.now()+x.toString)
  }
}

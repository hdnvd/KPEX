package ir.sweetsoft.kpex

import ir.sweetsoft.nlp.NLPTools

class Phrase(ApplicationContext:KpexContext) extends KpexClass(ApplicationContext:KpexContext){

  private[this] var _OccurrenceInTest: Int = 1

  def OccurrenceInTest: Int = _OccurrenceInTest

  def OccurrenceInTest_=(value: Int): Unit = {
    _OccurrenceInTest = value
  }

  private[this] var _TestID: Int = -1

  def TestID: Int = _TestID

  def TestID_=(value: Int): Unit = {
    _TestID = value
  }

  private[this] var _words: Seq[String] = Seq()
  private[this] var _maxWordCount: Int = 10

  def maxWordCount: Int = _maxWordCount

  def maxWordCount_=(value: Int): Unit = {
    _maxWordCount = value
  }

  def words: Seq[String] = _words

  def words_=(value: Seq[String]): Unit = {
    _words = value
  }

  def Phrase: String = {
    val nlp=new NLPTools(appContext)
    nlp.getPhraseFromWords(words)
  }

  def Phrase_=(value: String): Unit = {
    val nlp=new NLPTools(appContext)
    val thewords=nlp.GetStringWords(value)
    var NewPhraseWords:Seq[String]=Seq()
    val OriginalPhraseLength=thewords.length
    words=thewords
  }
//  def commitChanges(): Unit =
//  {
//    val thewords=words
//    var NewPhraseWords:Seq[String]=Seq()
//    val OriginalPhraseLength=thewords.length
//    if(OriginalPhraseLength>maxWordCount)
//    {
//      val NPHead=thewords.last
//      var ConsideredInSumWordCount=0
//      var DistanceToHeadSum:Double=0
//      for (i <- (OriginalPhraseLength - 1) to 0 by -1) {
//        if((OriginalPhraseLength-i)<=maxWordCount)
//          {
//            if(i!=OriginalPhraseLength-i)
//              {
//                val DistanceToHead=appContext.TotalWordEmbed.getEuclideanDistanceBetweenWords(thewords(i),NPHead,TestID)
//                if(DistanceToHead>0)
//                  {
//                    ConsideredInSumWordCount=ConsideredInSumWordCount+1
//                    DistanceToHeadSum=DistanceToHeadSum + DistanceToHead
//                  }
//
//              }
//            NewPhraseWords=NewPhraseWords :+ thewords(i)
//          }
//        else
//        {
//          val MeanDistanceToHead=DistanceToHeadSum/ConsideredInSumWordCount
//          val DistanceToHead=appContext.TotalWordEmbed.getEuclideanDistanceBetweenWords(thewords(i),NPHead,TestID)
//          if(DistanceToHead<=MeanDistanceToHead)
//            NewPhraseWords=NewPhraseWords :+ thewords(i)
////          SweetOut.printLine("Similarity From NPHead:"+NPHead+" of "+thewords(i) +" is "+Similarity,10)
//        }
//      }
//      //        SweetOut.printLine("NewPhrase:"+NewPhraseWords.length,10)
//      words=NewPhraseWords.reverse
//    }
//    else
//      words=thewords
//  }
}

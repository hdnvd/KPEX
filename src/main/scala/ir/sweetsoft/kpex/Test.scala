package ir.sweetsoft.kpex

import ir.sweetsoft.common.SweetOut
import ir.sweetsoft.nlp.NLPTools

class Test(ApplicationContext:KpexContext) extends KpexClass(ApplicationContext:KpexContext){

  private[this] var _TestID: Int = -1

  def TestID: Int = _TestID

  def TestID_=(value: Int): Unit = {
    _TestID = value
  }

  private[this] var _FullText: String = ""
  def FullText: String = _FullText
  def FullText_=(value: String): Unit = {
    _FullText = value
    val nlp=new NLPTools(appContext)
    val TestWords:Seq[String]=nlp.GetStringWords(value)
    Words=TestWords
  }

  private[this] var _Words: Seq[String] = Seq()
  def Words: Seq[String] = _Words
  def Words_=(value: Seq[String]): Unit = {
    _Words = value
  }
  private[this] var _GoldPhrases: Seq[Phrase] = Seq()
  def GoldPhrases: Seq[Phrase] = _GoldPhrases
  private[this] var _ExtractedPhrases: Seq[Phrase] = Seq()
  def ExtractedPhrases: Seq[Phrase] = _ExtractedPhrases
  private[this] var _ExtractedPosTags: Map[String, String] = Map()

  def ExtractedPosTags: Map[String, String] = _ExtractedPosTags

  def ExtractedPosTags_=(value: Map[String, String]): Unit = {
    val nlp=new NLPTools(appContext)
    SweetOut.printLine("setting ExtractedPhrase of "+TestID+" Length:"+value.size,1)
    value.foreach(PhrasePostag => {
      nlp.addToLemmatizationMap(PhrasePostag._1,TestID)
      val NormalizedPhrase=getNormalizedPhrase(PhrasePostag._1,TestID)
      SweetOut.printLine("ExtractedPhrase"+NormalizedPhrase,1)
      var thePhrase=new Phrase(appContext)
      thePhrase.Phrase=NormalizedPhrase
      _ExtractedPhrases=_ExtractedPhrases:+thePhrase
    })
    _ExtractedPosTags = value
  }

  private def getNormalizedPhrase(Phrase:String,TestID:Int): String =
  {
    val nlp=new NLPTools(appContext)
    val Words=nlp.GetStringWords(Phrase)
    var Result=""
    Words.foreach(Word=>
    {
      val lemmatizedWord=nlp.GetNormalizedAndLemmatizedWord(Word,TestID).toLowerCase()
      Result=Result+" "+nlp.removeSingleCharactersAndSeparateWithSpace(lemmatizedWord).trim
    })
    Result.trim.toLowerCase
  }

  def addGoldPhrase(Phrase: String): Unit =
  {
    val nlp=new NLPTools(appContext)
    val phraseRemovedExtras=nlp.removeSingleCharactersAndSeparateWithSpace(Phrase)
    nlp.addToLemmatizationMap(phraseRemovedExtras,TestID)
//    if(Phrase=="closed loop system")
      SweetOut.printLine("phrase:"+Phrase,1)
      SweetOut.printLine(getNormalizedPhrase(Phrase,TestID),1)
    val NormalizedPhrase=getNormalizedPhrase(Phrase,TestID)
    var thePhrase=new Phrase(appContext)
    thePhrase.Phrase=NormalizedPhrase
    _GoldPhrases=_GoldPhrases:+thePhrase
  }

}

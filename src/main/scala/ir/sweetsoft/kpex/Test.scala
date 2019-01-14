package ir.sweetsoft.kpex

import ir.sweetsoft.common.SweetOut
import ir.sweetsoft.nlp.NLPTools

import scala.util.hashing.MurmurHash3

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
    TestWords.foreach(word=>{
      appContext.NewIdentificationMap=appContext.NewIdentificationMap+(MurmurHash3.stringHash(word).toLong->word)
    })
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
      thePhrase.maxWordCount=3
      thePhrase.Phrase=NormalizedPhrase
      thePhrase.TestID=TestID
      thePhrase.words.foreach(word=>appContext.TotalWordEmbed.addNounPhraseWord(TestID,word))

//      SweetOut.printLine("HD:"+thePhrase.Phrase,10)
      if(!ExtractedPhrases.exists(Phrase=>Phrase.Phrase==thePhrase.Phrase))
        _ExtractedPhrases=_ExtractedPhrases:+thePhrase
      else
        {
//          SweetOut.printLine("HHAADDII:"+thePhrase.Phrase,10)
          _ExtractedPhrases=_ExtractedPhrases.map(Phrase=> {
            if(Phrase.Phrase == thePhrase.Phrase)
              Phrase.OccurrenceInTest=Phrase.OccurrenceInTest+1
            Phrase
          })
        }
    })
    _ExtractedPosTags = value

  }
//  def commitChanges(): Unit =
//  {
//    ExtractedPhrases.foreach(phrase=>phrase.commitChanges())
//  }
  private def getNormalizedPhrase(Phrase:String,TestID:Int): String =
  {
    val nlp=new NLPTools(appContext)
    val Words=nlp.GetStringWords(Phrase)
    var Result=""
    Words.foreach(Word=>
    {
      var lemmatizedWord=nlp.GetNormalizedAndLemmatizedWord(Word,TestID)
//      lemmatizedWord=nlp.stem(Word)
      val lemmatizedWordAfterRemovedExtras=nlp.removeSingleCharactersAndSeparateWithSpace(lemmatizedWord).trim
      if(lemmatizedWordAfterRemovedExtras!="")
      Result=Result+" "+lemmatizedWordAfterRemovedExtras
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

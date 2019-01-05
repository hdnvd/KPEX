package ir.sweetsoft.kpex

import ir.sweetsoft.nlp.{NLPTools, nltkAdapter}
import org.apache.spark.sql.SparkSession

class Corpus(ApplicationContext:KpexContext) extends KpexClass(ApplicationContext:KpexContext){
  private[this] var _Tests: Map[Int, Test] = Map()

  def Tests: Map[Int, Test] = _Tests


  private var _TotalTexts:String=""
  private var ExtractedPosTags:Map[Int,Map[String,String]]=Map()
  private[this] var _sparkSession: SparkSession = _

  def sparkSession: SparkSession = _sparkSession

  def sparkSession_=(value: SparkSession): Unit = {
    _sparkSession = value
  }

  def TotalText: String =
  {
    _TotalTexts
  }
  private def getTextNormalizedForm(inputString:String): String =
  {
    var NormalizedString = inputString.replace("\t", " ")
    if(!NormalizedString.substring(NormalizedString.length-1).equals("."))
      NormalizedString = NormalizedString+"."
    NormalizedString=NormalizedString.trim
    NormalizedString
  }
  def addTestContext(TestID:Int,text: String): Unit =
  {
    val nlp=new NLPTools(appContext)
    var CurrentTest=new Test(appContext)
    val NormalizedString=getTextNormalizedForm(text)
    CurrentTest.FullText=NormalizedString
    CurrentTest.TestID=TestID
    _Tests=_Tests+(TestID->CurrentTest)
    if(_TotalTexts!="")
      _TotalTexts=_TotalTexts+"\r\n"
    _TotalTexts=_TotalTexts+appContext.AppConfig.TEST_SEPARATOR_TEXT+"\r\n"+NormalizedString

    nlp.addToLemmatizationMap(NormalizedString,TestID)
  }
  def commitChanges(): Unit =
  {
    _TotalTexts=_TotalTexts+"\r\n"+appContext.AppConfig.TEST_SEPARATOR_TEXT+"\r\n"
    //      val AllNounPhrasesData=new textBlobAdapter(appContext).GetTotalNounPhrases(sparkSession,_TotalTexts)
    val AllNounPhrasesData=new nltkAdapter(appContext).GetTotalNounPhrases(sparkSession,_TotalTexts)
    AllNounPhrasesData.keySet.foreach(TestID=> {
      val AllExtractedPosTagsOfTest=AllNounPhrasesData(TestID)
      _Tests(TestID).ExtractedPosTags=AllExtractedPosTagsOfTest
    })
  }



}

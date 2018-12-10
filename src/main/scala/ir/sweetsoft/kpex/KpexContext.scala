package ir.sweetsoft.kpex

import ir.sweetsoft.WordEmbedding.WordEmbed

class KpexContext extends Serializable{
   var NounPhrases: Seq[String] = Seq()
   var NounPhrasePosTags: Map[String,String] = Map()
   var AllNounPhrases: Map[Int,Seq[String]] = Map()
   var AllNounPhrasePosTags: Map[Int,Map[String,String]] = Map()
   var WordFrequencies: Map[String,Int] = Map()
   var NewIdentificationMap: Map[Long, String] = Map()
   var MysqlConfigs: java.util.Properties = null
   var RealKeyPhrases: Map[Int,Seq[String]] = Map()
   var Exact_AlgorithmRate = 0d
   var Exact_TruePositivesCount = 0d

  var Approx_AlgorithmRate = 0d
  var Approx_TruePositivesCount = 0d

   var TotalApproxTruePositivesCount = 0d
  var TotalExactTruePositivesCount = 0d

  var TotalRealKeyphrasesCount = 0d
  var TotalExtractedKeyphrasesCount = 0d
   var inputString:Map[Int,String]=Map()
   var ExistentSimilarEdges:Map[(Long,Long),Int]=Map() //The Edges that Exists Between Similar Edges without need to our add
   var wordEmbeds:Map[Int,WordEmbed]=Map()
   var AppConfig:KpexConfig=new KpexConfig()
   var LemmatizationMaps:scala.collection.mutable.Map[Int,scala.collection.mutable.Map[String,String]]=scala.collection.mutable.Map()
   var FullResult:String=""
  def resetContextData(): Unit =
  {
    NounPhrases= Seq()
    WordFrequencies= Map()
    NewIdentificationMap= Map()
    Approx_AlgorithmRate = 0d
    Approx_TruePositivesCount = 0d

    Exact_AlgorithmRate = 0d
    Exact_TruePositivesCount = 0d
    ExistentSimilarEdges=Map()
  }
}

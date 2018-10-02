package ir.sweetsoft.kpex

import ir.sweetsoft.WordEmbedding.WordEmbed

class KpexContext extends Serializable{
   var NounPhrases: Seq[String] = Seq()
   var WordFrequencies: Map[String,Int] = Map()
//   var IdentificationMap: RDD[(Long, String)] = null
   var NewIdentificationMap: Map[Long, String] = Map()
   var MysqlConfigs: java.util.Properties = null
   var RealKeyPhrases: Seq[String] = null
   var algorithmRate = 0d
   var TruePositivesCount = 0d
   var ExistentSimilarEdges:Map[(Long,Long),Int]=Map() //The Edges that Exists Between Similar Edges without need to our add
   var wordEmbed:WordEmbed=null
   var AppConfig:KpexConfig=new KpexConfig()
   var LemmatizationMap:Map[String,String]=Map()
}

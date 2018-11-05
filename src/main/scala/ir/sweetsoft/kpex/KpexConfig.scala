package ir.sweetsoft.kpex

class KpexConfig extends Serializable {

  val METHOD_CLOSENESS = 1
  val METHOD_ECCENTERICITY = 2
  val METHOD_DEGREE = 3
  val METHOD_NE_RANK = 4

  val MEASURE_METHOD_EXACT=1
  val MEASURE_METHOD_APPROX=2
  val TEST_SEPARATOR_TEXT="kpex"


  val HDFS_MODE = 1
  val FILE_MODE = 2
  val MYSQL_MODE = 3

  val DataSetsPath = "/in/"
  var hasPosTagging = true
  var ResultKeywordsCount = 50
  var NounInfluence = 0.8
  var NounOutInfluence = 0.5
  var AdjectiveInfluence = 0.8
  var AdjectiveOutInfluence = 0.5
  var SingleOutput = false
  var ResultDirectory = ""
  var PostProcessSimilarityInfluenceFactor = 0.3
  var SimilarityMinThreshold = 0.3
//  var MinWordRate = 3
  var GraphImportanceMethod:Int = METHOD_CLOSENESS
  val WindowSize = 5
  var StorageType:Int = HDFS_MODE
  var DatabaseTestIDs:Seq[Int] = Seq()
  var DatabaseTestIDsString:String = ""
  var DatabaseContextURLs:Map[Int,String] = Map()
  var DatabaseContextTitles:Map[Int,String] = Map()
  var HasSimilarityEdgeWeighting=true
  var MeasurementMethod:Int=MEASURE_METHOD_APPROX


  var ResultDirectoryName:String="result"
  var DataSetDirectory:String = DataSetsPath
  var GraphPath:String  = DataSetDirectory + "/dataGraph.csv"
  var VisualGraphPath:String  = DataSetDirectory + "/dataGraph.html"
  var DataSetPath :String = DataSetDirectory + "data.txt"
  var DataSetKeyWordsPath:String  = DataSetDirectory + "keywords.txt"
  var DataSetFullResultPath:String  = DataSetDirectory + "keywords.txt"
  var DataSetRealKeyWordsPath:String  = DataSetDirectory + "realkeywords.txt"
  var IdentificationMapPath:String  = DataSetDirectory + "dataGraphIds.txt"
}

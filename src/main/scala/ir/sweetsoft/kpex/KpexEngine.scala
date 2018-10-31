package ir.sweetsoft.kpex

import java.io.BufferedOutputStream
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import ir.sweetsoft.common.SweetOut
import ir.sweetsoft.nlp.NLPTools
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

@SerialVersionUID(1145L)
class KpexEngine extends KpexContext {

  private def ReRateWordsBySimilarities(VertexMap: Seq[(Long, Double)]): Seq[(Long, Double)] = {
    var theVertexMap = VertexMap
    val nouns = NounPhrases.flatMap(np => {
      val allwords = np.split(" ").toSeq
      allwords
    })
    var VarNouns = nouns
    VertexMap.foreach(VR => {
      //      val rate = VR._1
      val vertexName = NewIdentificationMap(VR._1)

      if (nouns.contains(vertexName) && vertexName.length > 1) {
        theVertexMap = theVertexMap.map(SecondVR => {
          var Result = (SecondVR._1, SecondVR._2)
          if (SecondVR._1 != VR._1) {
            val vertexName2 = NewIdentificationMap(SecondVR._1)
            if (nouns.contains(vertexName2) && vertexName2.length > 1) {
              var similarity = wordEmbed.getSimilarityBetweenWords(vertexName, vertexName2)
              SweetOut.printLine("Similarity Between " + vertexName + " and " + vertexName2 + " is " + similarity,1)
              if (similarity > AppConfig.SimilarityMinThreshold && similarity < 2 && similarity != -2d && similarity!=Double.NaN) {
                SweetOut.printLine("PostProcessSimilarityInfluenceFactor is "+AppConfig.PostProcessSimilarityInfluenceFactor,1)
                Result = (SecondVR._1, SecondVR._2 + (VR._2 * similarity * AppConfig.PostProcessSimilarityInfluenceFactor))
              }
            }
          }
          Result
        })

      }
    }
    )
    theVertexMap
  }

  private def CalculateAndGetNounPhrasesRateByWordsRate(SortedVertexMap: Seq[(Long, Double)]): String = {
    val nlp=new NLPTools(this)
    var Result = ""
    val NPMap = NounPhrases.map {
      np =>

        val nlp=new NLPTools(this)
        val words = nlp.GetStringWords(np) // Get Single Words Of This NounPhrase
      var rate = 0d

        words.foreach { word => //Single Word From NounPhrase
          val wordRate = SortedVertexMap.filter { p =>
            var vertexName = NewIdentificationMap(p._1)
            vertexName = nlp.GetNormalizedAndLemmatizedWord(vertexName)
            vertexName.trim.toLowerCase.equals(word.toLowerCase.trim)
          }
          var DistanceSum=0d
          if (wordRate.nonEmpty) {
            var vertexName = NewIdentificationMap(wordRate.head._1)
//            DistanceSum=GetDistanceOfWordInPhrase(words,vertexName)
            //            AverageSimilarity=1d
            SweetOut.printLine("Sum Of Distance of " + vertexName + " In Phrase "+np+" Is " + DistanceSum,1)

            if(DistanceSum>0)
              rate = rate + wordRate.head._2 / DistanceSum
            else
              rate = rate + wordRate.head._2
          }
        }
        (np, rate)
    }
    val SortedNPMap = NPMap.sortBy(f => f._2).reverse
    var KeyWordIndex = 1
    SortedNPMap.foreach { np =>
      val TextLine = s"${KeyWordIndex}\t${np._1}\t${np._2}"
      KeyWordIndex += 1
      if (KeyWordIndex <= AppConfig.ResultKeywordsCount + 1) {
        var Matched=false
        RealKeyPhrases.foreach { RealKeyPhrase =>
          SweetOut.printLine("RK:"+RealKeyPhrase+" NP:"+np._1,2)
          var SuccessfulHit=false
          if(AppConfig.MeasurementMethod==AppConfig.MEASURE_METHOD_APPROX)
            SuccessfulHit=np._1.trim.toLowerCase.contains(RealKeyPhrase.toLowerCase.trim) || nlp.removeSingleCharactersAndSeparateWithSpace(np._1.trim.toLowerCase).contains(nlp.removeSingleCharactersAndSeparateWithSpace(RealKeyPhrase.toLowerCase.trim))
          else
            SuccessfulHit=np._1.trim.toLowerCase.equals(RealKeyPhrase.toLowerCase.trim)
          if (SuccessfulHit) {
            algorithmRate += SortedNPMap.length - KeyWordIndex
            TruePositivesCount = TruePositivesCount + 1
            Matched=true
          }

        }
        if(Matched)
          Result += "\n" + TextLine+"\t**"
        else
          Result += "\n" + TextLine

      }
    }

    Result
  }
  protected def GetDistanceOfWordInPhrase(PhraseWords:Seq[String], theWord:String): Double = {
          var DistanceSum:Double = 0d
          var AverageDistance:Double = 0d
          var validWordCount=0
    var InvalidWordCount=0
          PhraseWords.foreach(SecondaryWord => {
            if (!theWord.equals(SecondaryWord)) {
              val Distance = wordEmbed.getEuclideanDistanceBetweenWords(theWord, SecondaryWord)
              if (Distance != Double.NaN && Distance>0)
                {
                  DistanceSum = DistanceSum + Distance
                  validWordCount=validWordCount+1
                }
              else
                {
                  InvalidWordCount=InvalidWordCount+1
                  DistanceSum = DistanceSum + 1
                }

            }
          })
    if(validWordCount+InvalidWordCount>0)
      AverageDistance=DistanceSum/Math.pow(validWordCount+InvalidWordCount,2)
    AverageDistance
  }
  protected def RemoveExtraWordsFromNounPhrasesBySimilarity(): Unit = {
    NounPhrases = NounPhrases.flatMap(Phrase => {

      val nlp=new NLPTools(this)
      var PhraseWords: Seq[String] = nlp.GetStringWords(Phrase)
      PhraseWords.map(Word => nlp.GetNormalizedAndLemmatizedWord(Word))
      if (PhraseWords.length <= 2)
        Seq(Phrase)
      else {
        PhraseWords = PhraseWords.flatMap(Word => {
          var SimilaritySum = 0d
          var AverageSimilarity = 0d
          PhraseWords.foreach(SecondaryWord => {
            if (!Word.equals(SecondaryWord)) {
              val Similarity = wordEmbed.getSimilarityBetweenWords(Word, SecondaryWord)
              if (Similarity != Double.NaN)
                SimilaritySum = SimilaritySum + Similarity
            }
          })
          AverageSimilarity = SimilaritySum / (PhraseWords.length - 1)
          SweetOut.printLine("Average Of Similarity of " + Word + " In Phrase " + Phrase + " Is " + AverageSimilarity + " With Sum " + SimilaritySum,1)
          if (AverageSimilarity > 1)
            Seq(Word)
          else
            Seq()
        })
        var ResultPhrase = ""
        PhraseWords.foreach(Word => if (Word.trim.length > 0) ResultPhrase = ResultPhrase + Word + " ")
        Seq(ResultPhrase.trim)
      }

    })
  }


  protected def NormalizeString(inputString: String): String = {
    var ResultString = inputString.replace("\t", " ")
    ResultString
  }


  private def CalculateAndGetAllWordsRate(SortedVertexMap: Seq[(Long, Double)], realKeyWords: Seq[String]): String = {
    var Result = ""
    var KeyWordIndex = 1
    var AllWordsCount = SortedVertexMap.length
    for ((vertexId, closeness) <- SortedVertexMap) {
      var vertexName = NewIdentificationMap(vertexId)
      vertexName = vertexName.toString.trim()
      if (vertexName.length > 1 && vertexName != "nowhere") {
        realKeyWords.foreach { realKeyword =>
          if (vertexName.trim.toLowerCase.equals(realKeyword.toLowerCase.trim)) {
          }

        }
        val TextLine = s"${KeyWordIndex}\t${vertexName}\t${closeness}"
        KeyWordIndex += 1
        Result += "\n" + TextLine
      }
    }

    Result
  }

  def WriteOutput(spark: SparkSession, VertexMap: Seq[(Long, Double)], file: RDD[String]): Unit = {
    var vmap: Seq[(Long, Double)] = ReRateWordsBySimilarities(VertexMap)
    val SortedVertexMap = vmap.sortBy(-_._2)
    var Result = ""
    Result = Result + CalculateAndGetAllWordsRate(SortedVertexMap, RealKeyPhrases)
    Result = "\r\n**\t****************************************\t**"+ Result
    Result = "\r\n**\t****************************************\t**"+ Result
    Result = "\r\n**\t****************************************\t**"+ Result
    Result = CalculateAndGetNounPhrasesRateByWordsRate(SortedVertexMap)+ Result
    algorithmRate = algorithmRate / AppConfig.ResultKeywordsCount
    algorithmRate = algorithmRate / RealKeyPhrases.length
    val RateInt = (algorithmRate * 10000000).toInt

    val Precision:Double=    TruePositivesCount / Math.min(NounPhrases.length,AppConfig.ResultKeywordsCount)
    val Recall:Double=    TruePositivesCount / RealKeyPhrases.length
    var FScore:Double=0d
    if(Precision+Recall>0)
      FScore=2*Precision*Recall/(Precision+Recall)
    val dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss")
    val now = LocalDateTime.now()
    Result = "Result Keyword Count:" + AppConfig.ResultKeywordsCount + "\n" + Result
    Result = "Noun influence:" + AppConfig.NounInfluence + "\n" + Result
    Result = "Noun out-influence:" + AppConfig.NounOutInfluence + "\n" + Result
    Result = "Adjective influence:" + AppConfig.AdjectiveInfluence + "\n" + Result
    Result = "Adjective out-influence:" + AppConfig.AdjectiveOutInfluence + "\n" + Result
    Result = "Similarity threshold:" + AppConfig.SimilarityMinThreshold + "\n" + Result
    Result = "Similarity influence:" + AppConfig.PostProcessSimilarityInfluenceFactor + "\n" + Result
    Result = "Has Position Tagging:" + AppConfig.hasPosTagging + "\n" + Result
    Result = "Real Keyword Count:" + RealKeyPhrases.length + "\n" + Result
    Result = "Date:" + dtf.format(now) + "\n" + Result
    Result = "URL:" + AppConfig.DatabaseContextURL + "\n" + Result
    Result = "Title:" + AppConfig.DatabaseContextTitle + "\n" + Result
    Result = "3\tF-Score\t" +FScore + "\n" + Result
    Result = "2\tRecall\t" + Recall + "\n" + Result
    Result = "1\tPrecision\t" + Precision + "\n" + Result
    Result = "0\tRate\t" + RateInt + "\n" + Result
    val fs = FileSystem.newInstance(spark.sparkContext.hadoopConfiguration)
    val output = fs.create(new Path(AppConfig.DataSetKeyWordsPath))
    val os = new BufferedOutputStream(output)
    os.write(Result.getBytes("UTF-8"))
    os.close()
    fs.close()
    SweetOut.printLine("Rate:" + RateInt,4)
  }


  def getInputStringRDD(spark: SparkSession): RDD[String] = {
    val sc = spark.sparkContext
    var input: RDD[String] = null
    input = sc.textFile(AppConfig.DataSetPath)
    input
  }


  protected def Init(spark: SparkSession, args: Array[String]): Unit = {
    SweetOut.printLine("/Initiating...",5)

    /** ********Loading Parameters ************/
    // Sample Run Command: SweetKPE.jar hdfs postag 50 0.8 0.5 0.8 0.5 /in/mohammadi/data2/
    // Sample Run Command2: SweetKPE.jar nohdfs nopostag 50 0.8 0.5 0.8 0.5 /in/mohammadi/data2/
    // Sample Run Command Help: SweetKPE.jar nohdfs nopostag RESULTKEYWORD_COUNT NOUN_INFLUENCE NOUN_OUT_INFLUENCE ADJECTIVE_INFLUENCE ADJECTIVE_OUT_INFLUENCE DataSetDirectory

    LoadArgs(spark, args)
//    SweetOut.printLine("Dataset Directory:" + AppConfig.DataSetDirectory)
    /** ********End Of Loading Parameters ************/



    AppConfig.ResultDirectory = AppConfig.DataSetDirectory + "results/"+AppConfig.ResultDirectoryName
    AppConfig.GraphPath = AppConfig.ResultDirectory + "/DataGraph.csv"
    AppConfig.VisualGraphPath = AppConfig.ResultDirectory + "/DataGraph.html"
    AppConfig.DataSetPath = AppConfig.DataSetDirectory + "data.txt"
    AppConfig.DataSetRealKeyWordsPath = AppConfig.DataSetDirectory + "realkeywords.txt"
    if (!AppConfig.SingleOutput)
      AppConfig.DataSetKeyWordsPath = AppConfig.ResultDirectory + "/keywords" + AppConfig.ResultKeywordsCount + "-" + AppConfig.NounInfluence + "-" + AppConfig.NounOutInfluence + "-" + AppConfig.AdjectiveInfluence + "-" + AppConfig.AdjectiveOutInfluence + "-" + ".txt"
    else
      AppConfig.DataSetKeyWordsPath = AppConfig.ResultDirectory + "/keywords.txt"
    AppConfig.IdentificationMapPath = AppConfig.ResultDirectory + "/dataGraphIds.txt"
    if (AppConfig.StorageType == AppConfig.FILE_MODE) {

      if (!scala.tools.nsc.io.Path(AppConfig.ResultDirectory).exists)
        scala.tools.nsc.io.Path(AppConfig.ResultDirectory).createDirectory()
    }
    else {
      val fs = FileSystem.newInstance(spark.sparkContext.hadoopConfiguration)
      if (!fs.exists(new Path(AppConfig.ResultDirectory)))
        FileSystem.mkdirs(fs, new Path(AppConfig.ResultDirectory), FsPermission.getDefault)

      if (fs.exists(new Path(AppConfig.GraphPath)))
        fs.delete(new Path(AppConfig.GraphPath), true)
      if (fs.exists(new Path(AppConfig.VisualGraphPath)))
        fs.delete(new Path(AppConfig.VisualGraphPath), true)
      if (fs.exists(new Path(AppConfig.DataSetKeyWordsPath)))
        fs.delete(new Path(AppConfig.DataSetKeyWordsPath), true)
    }
    SweetOut.printLine("/Initiation Completed...",5)
  }

  protected def LoadArgs(spark: SparkSession, args: Array[String]): Unit = {
    if (args.length > 9) {
      if (args(2).toLowerCase.trim.equals("hdfs"))
        AppConfig.StorageType = AppConfig.HDFS_MODE
      else if (args(2).toLowerCase.trim.equals("mysql"))
        AppConfig.StorageType = AppConfig.MYSQL_MODE
      else
        AppConfig.StorageType = AppConfig.FILE_MODE
      if (args(3).toLowerCase.trim.equals("postag"))
        AppConfig.hasPosTagging = true
      else
        AppConfig.hasPosTagging = false

      AppConfig.ResultKeywordsCount = args(4).toInt
      AppConfig.NounInfluence = args(5).toFloat
      AppConfig.NounOutInfluence = args(6).toFloat
      AppConfig.AdjectiveInfluence = args(7).toFloat
      AppConfig.AdjectiveOutInfluence = args(8).toFloat

      AppConfig.DataSetDirectory = args(9)
    }
    var PostFix = ""
    if (!AppConfig.SingleOutput) {
      PostFix += "_" + AppConfig.GraphImportanceMethod
      if (AppConfig.hasPosTagging)
        PostFix += "_pos"
      PostFix += "_w" + AppConfig.WindowSize
    }
    AppConfig.ResultDirectoryName="result"+PostFix
    RealKeyPhrases = spark.sparkContext.textFile(AppConfig.DataSetRealKeyWordsPath).collect
  }
}
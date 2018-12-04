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

  protected var currentTestID:Int=0
  private def ReRateWordsBySimilarities(VertexMap: Seq[(Long, Double)]): Seq[(Long, Double)] = {
    var theVertexMap = VertexMap
    val nouns = NounPhrases.flatMap(np => {
      val allwords = np.split(" ").toSeq
      allwords
    })
    VertexMap.foreach(VR => {
      //      val rate = VR._1
      val vertexName = NewIdentificationMap(VR._1)

      if (nouns.contains(vertexName) && vertexName.length > 1) {
        theVertexMap = theVertexMap.map(SecondVR => {
          var Result = (SecondVR._1, SecondVR._2)
          if (SecondVR._1 != VR._1) {
            val vertexName2 = NewIdentificationMap(SecondVR._1)
            if (nouns.contains(vertexName2) && vertexName2.length > 1) {
              var similarity = wordEmbeds(currentTestID).getSimilarityBetweenWords(vertexName, vertexName2)
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

        val PosTagsString=NounPhrasePosTags(np)
        val nlp=new NLPTools(this)
        val words = nlp.GetStringWords(np) // Get Single Words Of This NounPhrase
        val PosTags = nlp.GetStringWords(PosTagsString) // Get Single Words Of This NounPhrase

//        SweetOut.printLine("NounPhrase IS:"+np,1)
        SweetOut.printLine("POSTAGString IS:"+PosTags,1)
      var rate = 0d

        var maxRateInPhrase=0d
        var LastWordRate=0d
        var PhraseRateVariance=0d
        var NounFound=false
        if(words.nonEmpty)
          {
            val NounPhraseHead=words.last
            for(i <- (words.length-1) to 0 by -1){
              val word=words(i)
              var posTag=""
              if(PosTags.length>i)
                posTag=PosTags(i)
              var posTagFactor=1d
              //          SweetOut.printLine("POSTAG IS:"+posTag,1)

              if(posTag.toUpperCase.trim.equals("NN") && !NounFound)
              {
                SweetOut.printLine("Head Of the Phrase "+np+" is: "+word,1)
                NounFound=true
                posTagFactor=1.0
              }

              //          else if(posTag.equals(""))
              //            SweetOut.printLine("ERROR FOUNDING POSTAG",1)
              //        words.foreach { word => //Single Word From NounPhrase
              val wordRate = SortedVertexMap.filter { p =>
                var vertexName = NewIdentificationMap(p._1)
                vertexName = nlp.GetNormalizedAndLemmatizedWord(vertexName,currentTestID)
                vertexName.trim.toLowerCase.equals(word.toLowerCase.trim)
              }
              var DistanceSum=0d
              if (wordRate.nonEmpty) {
                var vertexName = NewIdentificationMap(wordRate.head._1)
                //            DistanceSum=GetDistanceOfWordInPhrase(words,vertexName)
                //            AverageSimilarity=1d
                SweetOut.printLine("Sum Of Distance of " + vertexName + " In Phrase "+np+" Is " + DistanceSum,1)

                var thisWordRate=0d
                if(DistanceSum>0)
                  thisWordRate = wordRate.head._2 / DistanceSum
                else
                  thisWordRate= wordRate.head._2
                val DistanceFromNounPhraseHead = wordEmbeds(currentTestID).getSimilarityBetweenWords(word, NounPhraseHead)
                thisWordRate=thisWordRate+thisWordRate/DistanceFromNounPhraseHead
                if(LastWordRate!=0)
                  PhraseRateVariance=PhraseRateVariance+math.pow(thisWordRate-LastWordRate,2)
                LastWordRate=thisWordRate
                if(maxRateInPhrase<thisWordRate)
                  maxRateInPhrase=thisWordRate
                rate = rate + posTagFactor * thisWordRate
              }

              //          rate=rate+LastWordRate

            }
          }

//        rate=rate-PhraseRateVariance
//        rate=rate*maxRateInPhrase
        (np, rate)
    }
    val SortedNPMap = NPMap.sortBy(f => f._2).reverse
    var KeyWordIndex = 1
    var FoundKeyPhrases:Map[Int,String]=Map()
    SortedNPMap.foreach { np =>
      val TextLine = s"${KeyWordIndex}\t${np._1}\t${np._2}"
      KeyWordIndex += 1

        var Matched=false
        RealKeyPhrases(currentTestID).foreach { RealKeyPhrase =>
          if(!Matched && !FoundKeyPhrases.values.exists(_.equals(RealKeyPhrases))){
            SweetOut.printLine("RK:"+RealKeyPhrase+" NP:"+np._1,2)
            var SuccessfulHit=false
            if(AppConfig.MeasurementMethod==AppConfig.MEASURE_METHOD_APPROX)
              SuccessfulHit=np._1.trim.toLowerCase.contains(RealKeyPhrase.toLowerCase.trim) || nlp.removeSingleCharactersAndSeparateWithSpace(np._1.trim.toLowerCase).contains(nlp.removeSingleCharactersAndSeparateWithSpace(RealKeyPhrase.toLowerCase.trim))
            else
              SuccessfulHit=nlp.removeSingleCharactersAndSeparateWithSpace(np._1.trim.toLowerCase).equals(nlp.removeSingleCharactersAndSeparateWithSpace(RealKeyPhrase.toLowerCase.trim))
            if (SuccessfulHit) {
              if(KeyWordIndex <= AppConfig.ResultKeywordsCount + 1){
                algorithmRate += SortedNPMap.length - KeyWordIndex
                TruePositivesCount = TruePositivesCount + 1
              }
              FoundKeyPhrases=FoundKeyPhrases+(KeyWordIndex->RealKeyPhrase)
              Matched=true
            }
          }
        }
        Result =Result+"\n"
        if (KeyWordIndex >AppConfig.ResultKeywordsCount + 1)
          Result =Result+"--"

        if(Matched)
          Result += TextLine+"\t**"
        else
          Result += TextLine


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
              val Distance = wordEmbeds(currentTestID).getEuclideanDistanceBetweenWords(theWord, SecondaryWord)
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
      PhraseWords.map(Word => nlp.GetNormalizedAndLemmatizedWord(Word,currentTestID))
      if (PhraseWords.length <= 2)
        Seq(Phrase)
      else {
        PhraseWords = PhraseWords.flatMap(Word => {
          var SimilaritySum = 0d
          var AverageSimilarity = 0d
          PhraseWords.foreach(SecondaryWord => {
            if (!Word.equals(SecondaryWord)) {
              val Similarity = wordEmbeds(currentTestID).getSimilarityBetweenWords(Word, SecondaryWord)
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

  def PrepareLocalOutputData(spark: SparkSession, VertexMap: Seq[(Long, Double)], file: RDD[String]): Unit = {
    var vmap: Seq[(Long, Double)] = ReRateWordsBySimilarities(VertexMap)
    val SortedVertexMap = vmap.sortBy(-_._2)
    var Result = ""
    Result = Result + CalculateAndGetAllWordsRate(SortedVertexMap, RealKeyPhrases(currentTestID))
    Result = "\r\n**\t****************************************\t**"+ Result
    Result = "\r\n**\t****************************************\t**"+ Result
    Result = "\r\n**\t****************************************\t**"+ Result
    Result = CalculateAndGetNounPhrasesRateByWordsRate(SortedVertexMap)+ Result
    algorithmRate = algorithmRate / AppConfig.ResultKeywordsCount
    algorithmRate = algorithmRate / RealKeyPhrases(currentTestID).length
    val RateInt = (algorithmRate * 10000000).toInt

    val ExtractedKeyphrasesCount=Math.min(NounPhrases.length,AppConfig.ResultKeywordsCount)
    val RealKeyphrasesCount=RealKeyPhrases(currentTestID).length
    TotalTruePositivesCount=TotalTruePositivesCount+TruePositivesCount
    TotalRealKeyphrasesCount=TotalRealKeyphrasesCount+RealKeyphrasesCount
    TotalExtractedKeyphrasesCount=TotalExtractedKeyphrasesCount+ExtractedKeyphrasesCount
//    if(NounPhrases.length>AppConfig.ResultKeywordsCount)
//      SweetOut.printLine("NounPhrases Length Are More Than ResultKeywordsCount"+NounPhrases.length+"/"+AppConfig.ResultKeywordsCount,4)
    val Precision:Double=    TruePositivesCount /ExtractedKeyphrasesCount
    val Recall:Double=    TruePositivesCount / RealKeyphrasesCount
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
    Result = "Real Keyword Count:" + RealKeyPhrases(currentTestID).length + "\n" + Result
    Result = "Date:" + dtf.format(now) + "\n" + Result
    Result = "URL:" + AppConfig.DatabaseContextURLs(currentTestID) + "\n" + Result
    Result = "Title:" + AppConfig.DatabaseContextTitles(currentTestID) + "\n" + Result
    Result = "3\tF-Score\t" +FScore + "\n" + Result
    Result = "2\tRecall\t" + Recall + "\n" + Result
    Result = "1\tPrecision\t" + Precision + "\n" + Result
    Result = "0\tRate\t" + RateInt + "\n" + Result
    SweetOut.printLine("Rate:" + RateInt,4)
    SweetOut.printLine("Precision:"+Precision,4)
    SweetOut.printLine("Recall:"+Recall,4)
    SweetOut.printLine("FScore:"+FScore,4)
    FullResult=FullResult+"\r\n-------------------"+currentTestID+"-------------------\r\n"
    FullResult=FullResult+Result
//    SweetOut.printLine("Saving In:" + AppConfig.DataSetKeyWordsPath,1)
  }

  def WriteResultsToStorage(spark: SparkSession): Unit =
  {
    FullResult=FullResult+"\r\nTotalTruePositivesCount:"+TotalTruePositivesCount
    FullResult=FullResult+"\r\nTotalRealKeyphrasesCount:"+TotalRealKeyphrasesCount
    FullResult=FullResult+"\r\nTotalExtractedKeyphrasesCount:"+TotalExtractedKeyphrasesCount

    val Precision:Double=    TotalTruePositivesCount /TotalExtractedKeyphrasesCount
    val Recall:Double=    TotalTruePositivesCount / TotalRealKeyphrasesCount
    var FScore:Double=0d
    if(Precision+Recall>0)
      FScore=2*Precision*Recall/(Precision+Recall)
    SweetOut.printLine("Total Precision:"+Precision,4)
    SweetOut.printLine("Total Recall:"+Recall,4)
    SweetOut.printLine("Total FScore:"+FScore,4)

    FullResult=FullResult+"\r\nTotal Precision:"+Precision
    FullResult=FullResult+"\r\nTotal Recall:"+Recall
    FullResult=FullResult+"\r\nTotal FScore:"+FScore
    val fs = FileSystem.newInstance(spark.sparkContext.hadoopConfiguration)
    val output = fs.create(new Path(AppConfig.DataSetFullResultPath))
    val os = new BufferedOutputStream(output)
    os.write(FullResult.getBytes("UTF-8"))
    os.close()
    fs.close()
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




    SweetOut.printLine("/Initiation Completed...",5)
  }
  protected def LoadPerTestArgs(spark: SparkSession,TestID:Int): Unit =
  {
    AppConfig.ResultDirectoryName="result"+TestID
    AppConfig.ResultDirectory = "/in/results/"+AppConfig.ResultDirectoryName
    AppConfig.GraphPath = AppConfig.ResultDirectory + "/DataGraph.csv"
    AppConfig.VisualGraphPath = AppConfig.ResultDirectory + "/DataGraph.html"
    AppConfig.DataSetPath = AppConfig.DataSetDirectory + "data.txt"
    AppConfig.DataSetRealKeyWordsPath = AppConfig.DataSetDirectory + "realkeywords.txt"
    AppConfig.DataSetFullResultPath = "/in/results/FullResult.txt"
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
    AppConfig.DataSetKeyWordsPath = AppConfig.ResultDirectory + "/keywords" + TestID + ".txt"
  }
  protected def LoadArgs(spark: SparkSession, args: Array[String]): Unit = {
  }
}
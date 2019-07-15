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
    val nouns = CurrentCorpus.Tests(currentTestID).ExtractedPhrases.flatMap(np => {
      np.words
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
              var similarity = TotalWordEmbed.getSimilarityBetweenWords(vertexName, vertexName2)
              SweetOut.printLine("Similarity Between " + vertexName + " and " + vertexName2 + " is " + similarity, 1)
              if (similarity > AppConfig.SimilarityMinThreshold && similarity < 2 && similarity != -2d && similarity != Double.NaN) {
                SweetOut.printLine("PostProcessSimilarityInfluenceFactor is " + AppConfig.PostProcessSimilarityInfluenceFactor, 1)
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

  private def ReRateWordsByBeingInNounPhrases(VertexMap: Seq[(Long, Double)]): Seq[(Long, Double)] = {
    var theVertexMap = VertexMap
    val nouns = CurrentCorpus.Tests(currentTestID).ExtractedPhrases.flatMap(np => {
      np.words
    })

    var NounContainingPhrasesCountMap: Map[String, Int] = Map()
    nouns.foreach(noun => {
      var ContainingNounPhrasesCount = 0
      CurrentCorpus.Tests(currentTestID).ExtractedPhrases.foreach(NounPhrase => {

        var Found = false
        NounPhrase.words.foreach(word => {
          if (word.equals(noun))
            Found = true
        })
        if (Found)
          ContainingNounPhrasesCount = ContainingNounPhrasesCount + 1
      })
      NounContainingPhrasesCountMap = NounContainingPhrasesCountMap + (noun -> ContainingNounPhrasesCount)
    })
    theVertexMap = theVertexMap.map(WordCode => {
      val Word = NewIdentificationMap(WordCode._1)

      var ContainingNounPhrasesCount = 1
      if (NounContainingPhrasesCountMap.keys.exists(key => key == Word))
        ContainingNounPhrasesCount = ContainingNounPhrasesCount + NounContainingPhrasesCountMap(Word)
      val Result = (WordCode._1, WordCode._2 * ContainingNounPhrasesCount / CurrentCorpus.Tests(currentTestID).ExtractedPhrases.length + 1)
      Result
    })

    theVertexMap

  }
  private def CalculateAndGetNounPhrasesRateByMeanVectors(VertexMap: Seq[(Long, Double)]): String = {
    SweetOut.printLine("NounPhrase Count"+CurrentCorpus.Tests(currentTestID).ExtractedPhrases.length,1)
    val NPMap:Seq[(Phrase,Double)] = CurrentCorpus.Tests(currentTestID).ExtractedPhrases.flatMap {
      np =>
        if(np.OccurrenceInTest<1)
          Seq()
        else
          {
            val words = np.words
            var rate = 0d

            if (words.nonEmpty) {
              val NPHead=words.last
              //          val PhraseDistanceFromTest=TotalWordEmbed.getEuclideanDistanceFromCurrentTest(words)
              //          val PhraseDistanceFromCorpus=TotalWordEmbed.getEuclideanDistanceFromCorpus(words)
              //          rate =(DistanceFromCorpus/DistanceFromTest) * words.length
              val NounPhraseHead = words.last
              val NounPhraseLength = words.length
              for (i <- (NounPhraseLength - 1) to 0 by -1) {
                val word = words(i)
                val DistanceFromTest=TotalWordEmbed.getEuclideanDistanceFromCurrentTest(word)
                val DistanceFromPhraseHead=TotalWordEmbed.getEuclideanDistanceBetweenWords(word,NPHead)
                val DistanceFromCorpus=TotalWordEmbed.getEuclideanDistanceFromCorpus(word)
                val DistanceFromTestNounPhrases=TotalWordEmbed.getEuclideanDistanceFromCurrentTestNounPhrases(word)
                val DistanceFromCorpusNounPhrases=TotalWordEmbed.getEuclideanDistanceFromCorpusNounPhrases(word)
                //            SweetOut.printLine("Distances:"+DistanceFromTest+" "+DistanceFromCorpus+" "+DistanceFromTestNounPhrases+" "+DistanceFromCorpusNounPhrases+" ",10)
                var wordRate=DistanceFromCorpus/DistanceFromTest*DistanceFromCorpusNounPhrases/DistanceFromTestNounPhrases
                //            var wordRate=10/DistanceFromTestNounPhrases
//                SweetOut.printLine("DistanceFromPhraseHead"+DistanceFromPhraseHead,9)
                if(DistanceFromPhraseHead>0)
                  wordRate=wordRate+wordRate/DistanceFromPhraseHead
                else if(DistanceFromPhraseHead==0)
                  wordRate=wordRate+wordRate/4
                var OldRateOfWord=0d
                val OldWordWithRate=VertexMap.filter(aWord=>NewIdentificationMap(aWord._1)==word)
                if(OldWordWithRate.nonEmpty)
                  {
                    SweetOut.printLine("OldWord"+word,10)
                    OldRateOfWord=OldWordWithRate.last._2
                  }
                wordRate=wordRate+OldRateOfWord
                rate = rate +wordRate
              }
              //          rate=PhraseDistanceFromCorpus/PhraseDistanceFromTest
            }
            Seq((np, rate))
          }

        }
    GetNounPhrasesSortedByRate(NPMap)
  }

  private def CalculateAndGetNounPhrasesRateByWordsRate(SortedVertexMap: Seq[(Long, Double)]): String = {
    val NPMap:Seq[(Phrase,Double)] = CurrentCorpus.Tests(currentTestID).ExtractedPhrases.map {
      np =>

        val PosTagsString = CurrentCorpus.Tests(currentTestID).ExtractedPosTags(np.Phrase)
        val nlp = new NLPTools(this)
        val words = np.words
      val PosTags = nlp.GetStringWords(PosTagsString) // Get Single Words Of This NounPhrase

        //        SweetOut.printLine("NounPhrase IS:"+np,1)
        SweetOut.printLine("POSTAGString IS:" + PosTags, 1)
        var rate = 0d

        var maxRateInPhrase = 0d
        var LastWordRate = 0d
        var PhraseRateVariance = 0d
        var NounFound = false
        if (words.nonEmpty) {
          val NounPhraseHead = words.last
          val NounPhraseLength = words.length
          for (i <- (NounPhraseLength - 1) to 0 by -1) {
            val word = words(i)
            var posTag = ""
            if (PosTags.length > i)
              posTag = PosTags(i)
            var posTagFactor = 1d
            //          SweetOut.printLine("POSTAG IS:"+posTag,1)

            if (posTag.toUpperCase.trim.equals("NN") && !NounFound) {
              SweetOut.printLine("Head Of the Phrase " + np + " is: " + word, 1)
              NounFound = true
              posTagFactor = 1.0
            }

            //          else if(posTag.equals(""))
            //            SweetOut.printLine("ERROR FOUNDING POSTAG",1)
            //        words.foreach { word => //Single Word From NounPhrase
            val wordRate = SortedVertexMap.filter { p =>
              var vertexName = NewIdentificationMap(p._1)
              vertexName = nlp.GetNormalizedAndLemmatizedWord(vertexName, currentTestID)
              vertexName.trim.toLowerCase.equals(word.toLowerCase.trim)
            }
            var DistanceSum = 0d
            if (wordRate.nonEmpty) {
              var vertexName = NewIdentificationMap(wordRate.head._1)
              //            DistanceSum=GetDistanceOfWordInPhrase(words,vertexName)
              //            AverageSimilarity=1d
              SweetOut.printLine("Sum Of Distance of " + vertexName + " In Phrase " + np + " Is " + DistanceSum, 1)

              var thisWordRate = 0d
              if (DistanceSum > 0)
                thisWordRate = wordRate.head._2 / DistanceSum
              else
                thisWordRate = wordRate.head._2
              var DistanceFromNounPhraseHead = TotalWordEmbed.getSimilarityBetweenWords(word, NounPhraseHead)
              DistanceFromNounPhraseHead = math.pow(DistanceFromNounPhraseHead, 2)
              //              thisWordRate = thisWordRate + thisWordRate * DistanceFromNounPhraseHead
              if (LastWordRate != 0)
                PhraseRateVariance = PhraseRateVariance + math.pow(thisWordRate - LastWordRate, 2)
              LastWordRate = thisWordRate
              if (maxRateInPhrase < thisWordRate)
                maxRateInPhrase = thisWordRate
              rate = rate + posTagFactor * thisWordRate
            }

            //          rate=rate+LastWordRate

          }

          //            rate=rate/NounPhraseLength//Making Average
        }

        //        rate=rate-PhraseRateVariance
        //        rate=rate*maxRateInPhrase
        (np, rate)
    }
//    var ChangedNPMap:Seq[(String,Double)]=Seq()
//    var RemovedIndices:Seq[Int]=Seq()
//    println(NPMap.indices)
//    for (i <- NPMap.indices) {
//      var np: String = NPMap(i)._1
//      if(!RemovedIndices.contains(i))
//        {
//          var rate: Double = NPMap(i)._2
//
//          for (j <- i+1 until NPMap.length) {
//
//            if(nlp.GetStringWords(np).length<4 && nlp.GetStringWords(np).length>2 && !RemovedIndices.contains(j))
//              {
//                var np2: String = NPMap(j)._1
//                    var rate2: Double = NPMap(j)._2
//                    if (np2.contains(np)) {
//                      RemovedIndices=RemovedIndices:+j
//                      rate = rate + rate2
//                    }
//              }
//          }
//          ChangedNPMap=ChangedNPMap:+ (np, rate)
//        }
//    }
//    val SortedNPMap = ChangedNPMap.sortBy(f => f._2).reverse
    GetNounPhrasesSortedByRate(NPMap)
  }

  private def GetNounPhrasesSortedByRate(NounPhraseRateMap:Seq[(Phrase,Double)]): String =
  {
    var Result = ""
    val SortedNPMap = NounPhraseRateMap.sortBy(f => f._2).reverse
    var KeyWordIndex = 1
    var ExactFoundKeyPhrases: Map[Int, String] = Map()
    var ApproxFoundKeyPhrases: Map[Int, String] = Map()
    SortedNPMap.foreach { np =>
      val TextLine = s"$KeyWordIndex\t${np._1.Phrase}\t${np._2}"
      KeyWordIndex += 1

      var Approx_Matched = false
      var Exact_Matched = false
      var Exact_MatchedKeyPhrase = ""
      var Approx_MatchedKeyPhrase = ""
//      val NormalizedPhrase = nlp.removeSingleCharactersAndSeparateWithSpace(np._1.trim.toLowerCase)

      CurrentCorpus.Tests(currentTestID).GoldPhrases.foreach { RealKeyPhrase =>

                SweetOut.printLine("RK:" + RealKeyPhrase.Phrase + " NP:" + np._1.Phrase, 2)
        var APProxSuccessfulHit = false
        var ExactSuccessfulHit = false
//        val NormalizedRealPhrase = RealKeyPhrase
        APProxSuccessfulHit = np._1.Phrase.contains(RealKeyPhrase.Phrase) || np._1.Phrase.contains(RealKeyPhrase.Phrase)
        ExactSuccessfulHit = np._1.Phrase.equals(RealKeyPhrase.Phrase)
        if (!Exact_Matched && !ExactFoundKeyPhrases.values.exists(_.equals(RealKeyPhrase))) {
          if (ExactSuccessfulHit) {
            if (KeyWordIndex <= AppConfig.ResultKeywordsCount + 1) {
              Exact_AlgorithmRate += SortedNPMap.length - KeyWordIndex
              Exact_TruePositivesCount = Exact_TruePositivesCount + 1
            }
            ExactFoundKeyPhrases = ExactFoundKeyPhrases + (KeyWordIndex -> RealKeyPhrase.Phrase)
            Exact_Matched = true
            Exact_MatchedKeyPhrase = RealKeyPhrase.Phrase
          }
        }
        if (!Approx_Matched && !ApproxFoundKeyPhrases.values.exists(_.equals(RealKeyPhrase))) {
          if (APProxSuccessfulHit) {
            if (KeyWordIndex <= AppConfig.ResultKeywordsCount + 1) {
              Approx_AlgorithmRate += SortedNPMap.length - KeyWordIndex
              Approx_TruePositivesCount = Approx_TruePositivesCount + 1
            }
            Approx_MatchedKeyPhrase = RealKeyPhrase.Phrase
            ApproxFoundKeyPhrases = ApproxFoundKeyPhrases + (KeyWordIndex -> RealKeyPhrase.Phrase)
            Approx_Matched = true
          }
        }


      }
      Result = Result + "\n"
      if (KeyWordIndex > AppConfig.ResultKeywordsCount + 1)
        Result = Result + "--"

      if (Exact_Matched)
        Result += TextLine + "\t⇔"
      else if (Approx_Matched)
        Result += TextLine + "\t∍ " + Approx_MatchedKeyPhrase + " ⊖ " + np._1.Phrase.replace(Approx_MatchedKeyPhrase, "")
      else
        Result += TextLine


    }

    Result
  }
  protected def GetDistanceOfWordInPhrase(PhraseWords: Seq[String], theWord: String): Double = {
    var DistanceSum: Double = 0d
    var AverageDistance: Double = 0d
    var validWordCount = 0
    var InvalidWordCount = 0
    PhraseWords.foreach(SecondaryWord => {
      if (!theWord.equals(SecondaryWord)) {
        val Distance = TotalWordEmbed.getEuclideanDistanceBetweenWords(theWord, SecondaryWord)
        if (Distance != Double.NaN && Distance > 0) {
          DistanceSum = DistanceSum + Distance
          validWordCount = validWordCount + 1
        }
        else {
          InvalidWordCount = InvalidWordCount + 1
          DistanceSum = DistanceSum + 1
        }

      }
    })
    if (validWordCount + InvalidWordCount > 0)
      AverageDistance = DistanceSum / Math.pow(validWordCount + InvalidWordCount, 2)
    AverageDistance
  }

  protected def RemoveExtraWordsFromNounPhrasesBySimilarity(): Unit = {
    throw new Exception("This Method will not work after adding Corpus and Test")
    /*
    NounPhrases = NounPhrases.flatMap(Phrase => {

      val nlp = new NLPTools(this)
      var PhraseWords: Seq[String] = nlp.GetStringWords(Phrase)
      PhraseWords.map(Word => nlp.GetNormalizedAndLemmatizedWord(Word, currentTestID))
      if (PhraseWords.length <= 2)
        Seq(Phrase)
      else {
        PhraseWords = PhraseWords.flatMap(Word => {
          var SimilaritySum = 0d
          var AverageSimilarity = 0d
          PhraseWords.foreach(SecondaryWord => {
            if (!Word.equals(SecondaryWord)) {
              val Similarity = TotalWordEmbed.getSimilarityBetweenWords(Word, SecondaryWord)
              if (Similarity != Double.NaN)
                SimilaritySum = SimilaritySum + Similarity
            }
          })
          AverageSimilarity = SimilaritySum / (PhraseWords.length - 1)
          SweetOut.printLine("Average Of Similarity of " + Word + " In Phrase " + Phrase + " Is " + AverageSimilarity + " With Sum " + SimilaritySum, 1)
          if (AverageSimilarity > 1)
            Seq(Word)
          else
            Seq()
        })
        var ResultPhrase = ""
        PhraseWords.foreach(Word => if (Word.trim.length > 0) ResultPhrase = ResultPhrase + Word + " ")
        Seq(ResultPhrase.trim)
      }

    })*/
  }


  protected def NormalizeString(inputString: String): String = {
    var ResultString = inputString.replace("\t", " ")
    ResultString
  }


  private def CalculateAndGetAllWordsRate(SortedVertexMap: Seq[(Long, Double)], realKeyWords: Seq[Phrase]): String = {
    var Result = ""
    var KeyWordIndex = 1
    var AllWordsCount = SortedVertexMap.length
    for ((vertexId, closeness) <- SortedVertexMap) {
      var vertexName = NewIdentificationMap(vertexId)
      vertexName = vertexName.toString.trim()
      if (vertexName.length > 1 && vertexName != "nowhere") {
//        realKeyWords.foreach { realKeyword =>
//          if (vertexName.trim.toLowerCase.equals(realKeyword.Phrase.toLowerCase.trim)) {
//          }
//
//        }
        val TextLine = s"${KeyWordIndex}\t${vertexName}\t${closeness}"
        KeyWordIndex += 1
        Result += "\n" + TextLine
      }
    }

    Result
  }

  def PrepareLocalOutputData(spark: SparkSession, VertexMap: Seq[(Long, Double)], file: RDD[String]): Unit = {
    var vmap: Seq[(Long, Double)] = VertexMap
//    vmap = ReRateWordsBySimilarities(vmap)
//    vmap = ReRateWordsByBeingInNounPhrases(vmap)
    val SortedVertexMap = vmap.sortBy(-_._2)
    var Result = ""
    var WordRateResult=CalculateAndGetAllWordsRate(SortedVertexMap, CurrentCorpus.Tests(currentTestID).GoldPhrases)
    if(AppConfig.WordRatesInResult)
      Result = Result + WordRateResult
    Result = "\r\n**\t****************************************\t**" + Result
    Result = "\r\n**\t****************************************\t**" + Result
    Result = "\r\n**\t****************************************\t**" + Result
    Result = CalculateAndGetNounPhrasesRateByMeanVectors(vmap) + Result
    Exact_AlgorithmRate = Exact_AlgorithmRate / AppConfig.ResultKeywordsCount
    Exact_AlgorithmRate = Exact_AlgorithmRate / CurrentCorpus.Tests(currentTestID).GoldPhrases.length
    val ExactRateInt = (Exact_AlgorithmRate * 10000000).toInt

    Approx_AlgorithmRate = Approx_AlgorithmRate / AppConfig.ResultKeywordsCount
    Approx_AlgorithmRate = Approx_AlgorithmRate / CurrentCorpus.Tests(currentTestID).GoldPhrases.length
    val ApproxRateInt = (Approx_AlgorithmRate * 10000000).toInt

    val ExtractedKeyphrasesCount = Math.min(CurrentCorpus.Tests(currentTestID).ExtractedPhrases.length, AppConfig.ResultKeywordsCount)
    val RealKeyphrasesCount = CurrentCorpus.Tests(currentTestID).GoldPhrases.length
    TotalExactTruePositivesCount = TotalExactTruePositivesCount + Exact_TruePositivesCount
    TotalApproxTruePositivesCount = TotalApproxTruePositivesCount + Approx_TruePositivesCount
    TotalRealKeyphrasesCount = TotalRealKeyphrasesCount + RealKeyphrasesCount
    TotalExtractedKeyphrasesCount = TotalExtractedKeyphrasesCount + ExtractedKeyphrasesCount
    //    if(NounPhrases.length>AppConfig.ResultKeywordsCount)
    //      SweetOut.printLine("NounPhrases Length Are More Than ResultKeywordsCount"+NounPhrases.length+"/"+AppConfig.ResultKeywordsCount,4)
    val Exact_Precision: Double = Exact_TruePositivesCount / ExtractedKeyphrasesCount
    val Exact_Recall: Double = Exact_TruePositivesCount / RealKeyphrasesCount
    var Exact_FScore: Double = 0d
    if (Exact_Precision + Exact_Recall > 0)
      Exact_FScore = 2 * Exact_Precision * Exact_Recall / (Exact_Precision + Exact_Recall)

    val Approx_Precision: Double = Approx_TruePositivesCount / ExtractedKeyphrasesCount
    val Approx_Recall: Double = Approx_TruePositivesCount / RealKeyphrasesCount
    var Approx_FScore: Double = 0d
    if (Approx_Precision + Approx_Recall > 0)
      Approx_FScore = 2 * Approx_Precision * Approx_Recall / (Approx_Precision + Approx_Recall)

    val dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss")
    val now = LocalDateTime.now()
    Result = "-1\t-------------------Real Keywords Of " + currentTestID + "-------------------\t0\r\n" + Result
    val GoldKeyphrases = CurrentCorpus.Tests(currentTestID).GoldPhrases
    GoldKeyphrases.foreach(GKP => {

      Result = "**\t" + GKP.Phrase + "\t**\r\n" + Result
    })
    Result = "-1\t-------------------Real Keywords Of " + currentTestID + "-------------------\t0\r\n" + Result
    Result = "Result Keyword Count:" + AppConfig.ResultKeywordsCount + "\n" + Result
    Result = "Noun influence:" + AppConfig.NounInfluence + "\n" + Result
    Result = "Noun out-influence:" + AppConfig.NounOutInfluence + "\n" + Result
    Result = "Adjective influence:" + AppConfig.AdjectiveInfluence + "\n" + Result
    Result = "Adjective out-influence:" + AppConfig.AdjectiveOutInfluence + "\n" + Result
    Result = "Similarity threshold:" + AppConfig.SimilarityMinThreshold + "\n" + Result
    Result = "Similarity influence:" + AppConfig.PostProcessSimilarityInfluenceFactor + "\n" + Result
    Result = "Has Position Tagging:" + AppConfig.hasPosTagging + "\n" + Result
    Result = "Real Keyword Count:" + CurrentCorpus.Tests(currentTestID).GoldPhrases.length + "\n" + Result
    Result = "Date:" + dtf.format(now) + "\n" + Result
    Result = "URL:" + AppConfig.DatabaseContextURLs(currentTestID) + "\n" + Result
    Result = "Title:" + AppConfig.DatabaseContextTitles(currentTestID) + "\n" + Result

    SweetOut.printLine("Approx Rate:" + ApproxRateInt, 4)
    SweetOut.printLine("Approx Precision:" + Approx_Precision, 4)
    SweetOut.printLine("Approx Recall:" + Approx_Recall, 4)
    SweetOut.printLine("Approx FScore:" + Approx_FScore, 4)

    Result = "7\tApprox F-Score\t" + Approx_FScore + "\n" + Result
    Result = "6\tApprox Recall\t" + Approx_Recall + "\n" + Result
    Result = "5\tApprox Precision\t" + Approx_Precision + "\n" + Result
    Result = "4\tApprox Rate\t" + ApproxRateInt + "\n" + Result


    Result = "3\tExact F-Score\t" + Exact_FScore + "\n" + Result
    Result = "2\tExact Recall\t" + Exact_Recall + "\n" + Result
    Result = "1\tExact Precision\t" + Exact_Precision + "\n" + Result
    Result = "0\tExact Rate\t" + ExactRateInt + "\n" + Result

    SweetOut.printLine("Exact Rate:" + ExactRateInt, 4)
    SweetOut.printLine("Exact Precision:" + Exact_Precision, 4)
    SweetOut.printLine("Exact Recall:" + Exact_Recall, 4)
    SweetOut.printLine("Exact FScore:" + Exact_FScore, 4)

    FullResult = FullResult + "\r\n-------------------Real Keywords Of " + currentTestID + "-------------------\r\n"
    FullResult = FullResult + "\r\n-------------------" + currentTestID + "-------------------\r\n"
    FullResult = FullResult + Result
    //    SweetOut.printLine("Saving In:" + AppConfig.DataSetKeyWordsPath,1)
  }

  def WriteResultsToStorage(spark: SparkSession): Unit = {
    FullResult = FullResult + "\r\nTotalExactTruePositivesCount:" + TotalExactTruePositivesCount
    FullResult = FullResult + "\r\nTotalApproxTruePositivesCount:" + TotalApproxTruePositivesCount
    FullResult = FullResult + "\r\nTotalRealKeyphrasesCount:" + TotalRealKeyphrasesCount
    FullResult = FullResult + "\r\nTotalExtractedKeyphrasesCount:" + TotalExtractedKeyphrasesCount

    val Exact_Precision: Double = TotalExactTruePositivesCount / TotalExtractedKeyphrasesCount
    val Exact_Recall: Double = TotalExactTruePositivesCount / TotalRealKeyphrasesCount
    var Exact_FScore: Double = 0d
    if (Exact_Precision + Exact_Recall > 0)
      Exact_FScore = 2 * Exact_Precision * Exact_Recall / (Exact_Precision + Exact_Recall)
    SweetOut.printLine("Total Exact_Precision:" + Exact_Precision, 4)
    SweetOut.printLine("Total Exact_Recall:" + Exact_Recall, 4)
    SweetOut.printLine("Total Exact_FScore:" + Exact_FScore, 4)

    FullResult = FullResult + "\r\nTotal Exact_Precision:" + Exact_Precision
    FullResult = FullResult + "\r\nTotal Exact_Recall:" + Exact_Recall
    FullResult = FullResult + "\r\nTotal Exact_FScore:" + Exact_FScore

    val Approx_Precision: Double = TotalApproxTruePositivesCount / TotalExtractedKeyphrasesCount
    val Approx_Recall: Double = TotalApproxTruePositivesCount / TotalRealKeyphrasesCount
    var Approx_FScore: Double = 0d
    if (Approx_Precision + Approx_Recall > 0)
      Approx_FScore = 2 * Approx_Precision * Approx_Recall / (Approx_Precision + Approx_Recall)
    SweetOut.printLine("Total Approx_Precision:" + Approx_Precision, 4)
    SweetOut.printLine("Total Approx_Recall:" + Approx_Recall, 4)
    SweetOut.printLine("Total Approx_FScore:" + Approx_FScore, 4)

    FullResult = FullResult + "\r\nTotal Approx_Precision:" + Approx_Precision
    FullResult = FullResult + "\r\nTotal Approx_Recall:" + Approx_Recall
    FullResult = FullResult + "\r\nTotal Approx_FScore:" + Approx_FScore

    val fs = FileSystem.newInstance(spark.sparkContext.hadoopConfiguration)
    val output = fs.create(new Path(AppConfig.DataSetFullResultPath))
    val os = new BufferedOutputStream(output)
    os.write(FullResult.getBytes("UTF-8"))
    os.close()
    fs.close()
  }

//  def getInputStringRDD(spark: SparkSession): RDD[String] = {
//    val sc = spark.sparkContext
//    var input: RDD[String] = null
//    input = sc.textFile(AppConfig.DataSetPath)
//    input
//  }


  protected def Init(spark: SparkSession, args: Array[String]): Unit = {
    SweetOut.printLine("/Initiating...", 5)

    /** ********Loading Parameters ************/
    // Sample Run Command: SweetKPE.jar hdfs postag 50 0.8 0.5 0.8 0.5 /in/mohammadi/data2/
    // Sample Run Command2: SweetKPE.jar nohdfs nopostag 50 0.8 0.5 0.8 0.5 /in/mohammadi/data2/
    // Sample Run Command Help: SweetKPE.jar nohdfs nopostag RESULTKEYWORD_COUNT NOUN_INFLUENCE NOUN_OUT_INFLUENCE ADJECTIVE_INFLUENCE ADJECTIVE_OUT_INFLUENCE DataSetDirectory

    LoadArgs(spark, args)
    //    SweetOut.printLine("Dataset Directory:" + AppConfig.DataSetDirectory)
    /** ********End Of Loading Parameters ************/


    SweetOut.printLine("/Initiation Completed...", 5)
  }

  protected def LoadPerTestArgs(spark: SparkSession, TestID: Int): Unit = {
    AppConfig.ResultDirectoryName = "result" + TestID
    AppConfig.ResultDirectory = "/in/results/" + AppConfig.ResultDirectoryName
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
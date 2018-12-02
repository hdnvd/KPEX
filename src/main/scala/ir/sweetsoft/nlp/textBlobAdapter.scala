package ir.sweetsoft.nlp

import ir.sweetsoft.common.SweetOut
import ir.sweetsoft.kpex.{KpexClass, KpexContext}
import org.apache.spark.sql.SparkSession

class textBlobAdapter(ApplicationContext:KpexContext) extends KpexClass(ApplicationContext:KpexContext) {

//  def GetNounPhrases(spark: SparkSession, inputString: String,TestID:Int) : Seq[String] = {
//    SweetOut.printLine("Loading Noun Phrases...",1)
//
//    val dataRDD = spark.sparkContext.makeRDD(Seq(inputString.toLowerCase))
//    val scriptPath = "python /home/hduser/FinalProject/textblobnp.py"
//    val pipeRDD = dataRDD.pipe(scriptPath)
//    SweetOut.printLine("Loaded Noun Phrases From Python",1)
//    var NounPhrases:Seq[String] = pipeRDD.collect
//    SweetOut.printLine("Collected Noun Phrases",1)
//    NounPhrases=NounPhrases.distinct
//    var ProcessedNPs: Seq[String] = Seq()
//    NounPhrases.foreach {
//      np =>
//        val nlp=new NLPTools(appContext)
//        var Words=nlp.GetStringWords(np)
//
//        Words=Words.flatMap(Word=>
//        {
//          val wd=nlp.GetNormalizedAndLemmatizedWord(Word,TestID)
//          if(wd.trim!="")
//            Seq(wd)
//          else
//            Seq()
//        })
//        var npSeqStr = ""
//        Words.foreach {
//          n =>
//            if (npSeqStr != "")
//              npSeqStr = npSeqStr + " "
//            npSeqStr = npSeqStr + n
//        }
//        ProcessedNPs = ProcessedNPs ++ Seq(npSeqStr)
//    }
//    NounPhrases = ProcessedNPs.distinct
//    SweetOut.printLine("Loading Noun Phrases Completed",1)
//    NounPhrases
//  }
  def GetTotalNounPhrases(spark: SparkSession,TotalInputString: String) : Map[Int,Map[String,String]] = {
    SweetOut.printLine("Loading Noun Phrases...",1)
    SweetOut.printLine(TotalInputString,1)

    val dataRDD = spark.sparkContext.makeRDD(Seq(TotalInputString.toLowerCase))
    val scriptPath = "python /home/hduser/FinalProject/textblobnp.py"
    val pipeRDD = dataRDD.pipe(scriptPath)
    SweetOut.printLine("Loaded Noun Phrases From Python",1)
    var TotalNounPhrases:Seq[String] = pipeRDD.collect
    SweetOut.printLine("Collected Noun Phrases",1)
//    NounPhrases=NounPhrases.distinct
    var ProcessedNPs: Seq[String] = Seq()
    var TestID = -1
//    var AllNounPhrases: Map[Int,Seq[String]] = Map()
    var AllNounPhrasePosTags: Map[Int,Map[String,String]] = Map()
  var TestIndex=0
    TotalNounPhrases.foreach {
      np =>
        SweetOut.printLine("NP Is "+np,1)

        if(np.contains(ApplicationContext.AppConfig.TEST_SEPARATOR_TEXT))
          {
            if(TestID>0)
              {
                SweetOut.printLine("TestID Is "+TestID,1)
                val DistinctProcessedNPs=ProcessedNPs.distinct
                var NpPosTags:Map[String,String]=Map()
                DistinctProcessedNPs.foreach(np=>
                  {
                    val npWords=np.split("\\s")
                    var PosTags=""
                    npWords.foreach(word=>{PosTags=PosTags+" "+"NN"})
                    NpPosTags=NpPosTags+(np.trim->PosTags.trim)
                  }
                )
//                AllNounPhrases=AllNounPhrases+(TestID->DistinctProcessedNPs)
                AllNounPhrasePosTags=AllNounPhrasePosTags+(TestID->NpPosTags)
                ProcessedNPs=Seq()
              }
            if(TestIndex<ApplicationContext.AppConfig.DatabaseTestIDs.length)
              TestID=ApplicationContext.AppConfig.DatabaseTestIDs(TestIndex)
            TestIndex=TestIndex+1
          }
        else
          {
            val nlp=new NLPTools(appContext)
            var Words=nlp.GetStringWords(np)

            Words=Words.flatMap(Word=>
            {
              val wd=nlp.GetNormalizedAndLemmatizedWord(Word,TestID)
//              val wd=""
              if(wd.trim!="")
                Seq(wd)
              else
                Seq()
            })
            var npSeqStr = ""
            Words.foreach {
              n =>
                if (npSeqStr != "")
                  npSeqStr = npSeqStr + " "
                npSeqStr = npSeqStr + n
            }
            ProcessedNPs = ProcessedNPs ++ Seq(npSeqStr)
          }

    }
//    NounPhrases = ProcessedNPs.distinct
    SweetOut.printLine("Loading Noun Phrases Completed",1)
    AllNounPhrasePosTags
  }
}

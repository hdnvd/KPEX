package ir.sweetsoft.nlp

import ir.sweetsoft.common.SweetOut
import ir.sweetsoft.kpex.{KpexClass, KpexContext}
import org.apache.spark.sql.SparkSession

class nltkAdapter(ApplicationContext:KpexContext) extends KpexClass(ApplicationContext:KpexContext)
{
//  def GetNounPhrases(spark: SparkSession, inputString: String,TestID:Int) : Seq[String] = {
//    println("Loading Noun Phrases...")
//
//    val dataRDD = spark.sparkContext.makeRDD(Seq(inputString.toLowerCase))
//    val scriptPath = "python /home/hduser/FinalProject/np.py"
//    val pipeRDD = dataRDD.pipe(scriptPath)
//    var NounPhrases:Seq[String] = pipeRDD.collect.distinct
//    var ProcessedNPs: Seq[String] = Seq()
//    NounPhrases.foreach {
//      np =>
//
//        val nlp=new NLPTools(appContext)
//        var Words=nlp.GetStringWords(np)
//        Words=Words.flatMap(Word=>
//        {
//          val wd=nlp.GetNormalizedAndLemmatizedWord(Word,TestID)
//          if(wd.trim!="")
//            Seq(wd)
//          else
//            Seq()
//        })
//        //        val npSeq = nlp.plainTextToLemmas(np, false)
//        var npSeqStr = ""
//        Words.foreach {
//          n =>
//            //            println("NPWord is:"+n)
//            if (npSeqStr != "")
//              npSeqStr = npSeqStr + " "
//            npSeqStr = npSeqStr + n
//        }
//        ProcessedNPs = ProcessedNPs ++ Seq(npSeqStr)
//    }
//    NounPhrases = ProcessedNPs.distinct
//    NounPhrases
//  }
  def GetTotalNounPhrases(spark: SparkSession,TotalInputString: String) : Map[Int,Seq[String]] = {
    SweetOut.printLine("Loading Noun Phrases...",1)
    SweetOut.printLine(TotalInputString,1)

    val dataRDD = spark.sparkContext.makeRDD(Seq(TotalInputString.trim.toLowerCase))
    val scriptPath = "python /home/hduser/FinalProject/np.py"
    val pipeRDD = dataRDD.pipe(scriptPath)
    SweetOut.printLine("Loaded Noun Phrases From Python",1)
    var TotalNounPhrases:Seq[String] = pipeRDD.collect
    SweetOut.printLine("Collected Noun Phrases",1)
    //    NounPhrases=NounPhrases.distinct
    var ProcessedNPs: Seq[String] = Seq()
    var TestID = -1
    var AllNounPhrases: Map[Int,Seq[String]] = Map()
  var TestIndex=0
    TotalNounPhrases.foreach {
      nounphrase =>

        val np=nounphrase.toLowerCase
        SweetOut.printLine("NP Is "+np,1)

        if(np.contains(ApplicationContext.AppConfig.TEST_SEPARATOR_TEXT))
        {
          if(TestID>0)
          {
            SweetOut.printLine("TestID Is "+TestID,1)
            AllNounPhrases=AllNounPhrases+(TestID->ProcessedNPs.distinct)
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
    AllNounPhrases
  }
}

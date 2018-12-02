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
  def GetTotalNounPhrases(spark: SparkSession,TotalInputString: String) : Map[Int,Map[String,String]] = {
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
//    var AllNounPhrases: Map[Int,Seq[String]] = Map()
  var AllNounPhrasePosTags: Map[Int,Map[String,String]]  = Map()
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
            var DistinctProcessedNPs=ProcessedNPs.distinct
            var NpPosTags:Map[String,String]=Map()
            DistinctProcessedNPs.foreach(np=>
            {
              SweetOut.printLine("NP:"+np,1)
              val npWords=np.split(" ")
              var PosTags=""
              var PureWord=""
              var QueueWordsCount=0
              npWords.foreach(word=>{
                SweetOut.printLine("Word:"+word,1)
                val WordParts=word.split("/")
                PureWord=PureWord+" "+WordParts(0)
                if(WordParts.length<2)
                  QueueWordsCount=QueueWordsCount+1
                else{
                  for(i<-1 to (QueueWordsCount+1))
                      PosTags=PosTags+" "+WordParts(1)
                  QueueWordsCount=0
                }
              })
              SweetOut.printLine("PosTag:"+PosTags,1)
              NpPosTags=NpPosTags+(PureWord.trim->PosTags.trim)
            }
            )
//            DistinctProcessedNPs=DistinctProcessedNPs.map(np=>
//            {
//              val npWords=np.split(" ")
//              var PureWord=""
//              npWords.foreach(word=>{
//                val WordParts=word.split("/")
//                PureWord=PureWord+" "+WordParts(0)
//              })
//              SweetOut.printLine("PureWord:"+PureWord,1)
//              PureWord.trim
//            }
//            )

//            AllNounPhrases=AllNounPhrases+(TestID->DistinctProcessedNPs)
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

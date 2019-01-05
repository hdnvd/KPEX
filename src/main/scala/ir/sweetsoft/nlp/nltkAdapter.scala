package ir.sweetsoft.nlp

import ir.sweetsoft.common.SweetOut
import ir.sweetsoft.kpex.{KpexClass, KpexContext}
import org.apache.spark.sql.SparkSession

class nltkAdapter(ApplicationContext:KpexContext) extends KpexClass(ApplicationContext:KpexContext)
{
  def GetTotalNounPhrases(spark: SparkSession,TotalInputString: String) : Map[Int,Map[String,String]] = {
    SweetOut.printLine("Loading Noun Phrases...",1)
    SweetOut.printLine(TotalInputString,1)

    val dataRDD = spark.sparkContext.makeRDD(Seq(TotalInputString.trim.toLowerCase))
    val scriptPath = "python /home/hduser/FinalProject/np.py"
    val pipeRDD = dataRDD.pipe(scriptPath)
    SweetOut.printLine("Loaded Noun Phrases From Python",1)
    var TotalNounPhrases:Seq[String] = pipeRDD.collect
    SweetOut.printLine("Collected Noun Phrases",1)
    var ProcessedNPs: Seq[String] = Seq()
    var TestID = -1
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
                val WordParts=word.split("_")
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
            AllNounPhrasePosTags=AllNounPhrasePosTags+(TestID->NpPosTags)
            ProcessedNPs=Seq()
          }
          if(TestIndex<ApplicationContext.AppConfig.DatabaseTestIDs.length)
            TestID=ApplicationContext.AppConfig.DatabaseTestIDs(TestIndex)
          SweetOut.printLine("TestID"+TestID,1)
          TestIndex=TestIndex+1
        }
        else
        {
          val nlp=new NLPTools(appContext)
          var Words=nlp.GetStringWords(np)

          Words=Words.flatMap(Word=>
          {
            val wd=nlp.GetNormalizedAndLemmatizedWord(Word,TestID)
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
    SweetOut.printLine("Loading Noun Phrases Completed",1)
    SweetOut.printLine("AllNounPhrasePosTags "+AllNounPhrasePosTags.size,1)
    AllNounPhrasePosTags
  }
}

package ir.sweetsoft.nlp

import ir.sweetsoft.common.SweetOut
import ir.sweetsoft.kpex.{KpexClass, KpexContext}
import org.apache.spark.sql.SparkSession

class textBlobAdapter(ApplicationContext:KpexContext) extends KpexClass(ApplicationContext:KpexContext) {

  def GetNounPhrases(spark: SparkSession, inputString: String) : Seq[String] = {
    SweetOut.printLine("Loading Noun Phrases...",1)

    val dataRDD = spark.sparkContext.makeRDD(Seq(inputString.toLowerCase))
    val scriptPath = "python /home/hduser/FinalProject/textblobnp.py"
    val pipeRDD = dataRDD.pipe(scriptPath)
    SweetOut.printLine("Loaded Noun Phrases From Python",1)
    var NounPhrases:Seq[String] = pipeRDD.collect
    SweetOut.printLine("Collected Noun Phrases",1)
    NounPhrases=NounPhrases.distinct
    var ProcessedNPs: Seq[String] = Seq()
    NounPhrases.foreach {
      np =>
//        println("NP is:"+np)
        val nlp=new NLPTools(appContext)
        var Words=nlp.GetStringWords(np)

        Words=Words.flatMap(Word=>
        {
          val wd=nlp.GetNormalizedAndLemmatizedWord(Word)
          if(wd.trim!="")
            Seq(wd)
          else
            Seq()
        })
//        val npSeq = nlp.plainTextToLemmas(np, false)
        var npSeqStr = ""
        Words.foreach {
          n =>
//            println("NPWord is:"+n)
            if (npSeqStr != "")
              npSeqStr = npSeqStr + " "
            npSeqStr = npSeqStr + n
        }
        ProcessedNPs = ProcessedNPs ++ Seq(npSeqStr)
    }
    NounPhrases = ProcessedNPs.distinct
    SweetOut.printLine("Loading Noun Phrases Completed",1)
    NounPhrases
  }
}

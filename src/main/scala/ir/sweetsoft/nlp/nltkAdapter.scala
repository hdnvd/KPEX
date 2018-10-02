package ir.sweetsoft.nlp

import ir.sweetsoft.kpex.{KpexClass, KpexContext}
import org.apache.spark.sql.SparkSession

class nltkAdapter(ApplicationContext:KpexContext) extends KpexClass(ApplicationContext:KpexContext)
{
  def GetNounPhrases(spark: SparkSession, inputString: String) : Seq[String] = {
    println("Loading Noun Phrases...")

    val dataRDD = spark.sparkContext.makeRDD(Seq(inputString.toLowerCase))
    val scriptPath = "python /home/hduser/FinalProject/np.py"
    val pipeRDD = dataRDD.pipe(scriptPath)
    var NounPhrases:Seq[String] = pipeRDD.collect.distinct
    var ProcessedNPs: Seq[String] = Seq()
    NounPhrases.foreach {
      np =>

        val nlp=new NLPTools(appContext)
        val words=nlp.GetStringWords(np)
                var npSeqStr = ""
        words.foreach {
                  word =>
                    if (npSeqStr != "")
                      npSeqStr = npSeqStr + " "
                    npSeqStr = npSeqStr + nlp.GetNormalizedAndLemmatizedWord(word)
                }
//        val npSeq = nlp.plainTextToLemmas(np)
//        var npSeqStr = ""
//        npSeq.foreach {
//          n =>
//            if (npSeqStr != "")
//              npSeqStr = npSeqStr + " "
//            npSeqStr = npSeqStr + n
//        }
        ProcessedNPs = ProcessedNPs ++ Seq(npSeqStr)
    }
    NounPhrases = ProcessedNPs.distinct
    NounPhrases
  }
}

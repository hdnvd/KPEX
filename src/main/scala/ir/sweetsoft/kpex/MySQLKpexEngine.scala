package ir.sweetsoft.kpex

import java.util.Properties

import ir.sweetsoft.common.SweetOut
import ir.sweetsoft.nlp.NLPTools
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

@SerialVersionUID(1145L)
class MySQLKpexEngine extends KpexEngine  {

//  private var inputString = ""
//  private var ContextID = 0

  override protected def LoadArgs(spark: SparkSession, args: Array[String]): Unit = {
    if (args.length > 2) {
      AppConfig.DatabaseTestIDsString=args(2)
SweetOut.printLine("twstID"+AppConfig.DatabaseTestIDsString,10)
      var TestIDsInfo=AppConfig.DatabaseTestIDsString.split("-")
      var TestIDFrom=TestIDsInfo(0).toInt
      var TestIDTo=TestIDFrom
      var TestIDStep=1
      if(TestIDsInfo.length>2){

        TestIDTo=TestIDsInfo(1).toInt
        TestIDStep=TestIDsInfo(2).toInt
      }
      AppConfig.DatabaseTestIDs=Seq()
      for(i<-TestIDFrom to TestIDTo by TestIDStep)
      {
        if(AppConfig.DatabaseTestIDsString!="")
          AppConfig.DatabaseTestIDsString=AppConfig.DatabaseTestIDsString+","
        AppConfig.DatabaseTestIDsString=AppConfig.DatabaseTestIDsString+i
        AppConfig.DatabaseTestIDs=AppConfig.DatabaseTestIDs:+i
      }
      AppConfig.StorageType = AppConfig.MYSQL_MODE
      MysqlConfigs = new Properties
      MysqlConfigs.put("driver", "com.mysql.jdbc.Driver")
      MysqlConfigs.put("url", "jdbc:mysql://localhost:3306/keyphraseex?useSSL=false")
      MysqlConfigs.put("user", "root")
      MysqlConfigs.put("password", "Persian1147%")

      val sc = spark.sparkContext
      var input: RDD[String] = null

      val test = spark.read
        .format("jdbc")
        .option("url", MysqlConfigs.getProperty("url"))
        .option("user", MysqlConfigs.getProperty("user"))
        .option("password", MysqlConfigs.getProperty("password"))
        .option("dbtable", "(SELECT * FROM sweetp_kpex_test WHERE id in (" + AppConfig.DatabaseTestIDsString + ")) test")
        .load()
      val testCollected = test.collect()

      AppConfig.NounInfluence = testCollected(0).getDouble(4)
      AppConfig.NounOutInfluence = testCollected(0).getDouble(5)
      AppConfig.AdjectiveInfluence = testCollected(0).getDouble(6)
      AppConfig.AdjectiveOutInfluence = testCollected(0).getDouble(7)
      AppConfig.ResultKeywordsCount = testCollected(0).getInt(10)
      AppConfig.hasPosTagging = testCollected(0).getBoolean(14)
      AppConfig.HasSimilarityEdgeWeighting = testCollected(0).getBoolean(15)
      AppConfig.SimilarityMinThreshold = testCollected(0).getDouble(8)
      AppConfig.PostProcessSimilarityInfluenceFactor = testCollected(0).getDouble(9)
      val MethodID = testCollected(0).getInt(16)
      AppConfig.GraphImportanceMethod = MethodID
      SweetOut.printLine("Method ID:"+AppConfig.GraphImportanceMethod,1)

//      var TotalInputString=""
      testCollected.foreach(test=>
        {
          val ContextID = test.getInt(11)
          val TestID=test.getInt(0)
          currentTestID=TestID
          SweetOut.printLine("ContextID : " + ContextID,1)
          val context = spark.read
            .format("jdbc")
            .option("url", MysqlConfigs.getProperty("url"))
            .option("user", MysqlConfigs.getProperty("user"))
            .option("password", MysqlConfigs.getProperty("password"))
            .option("dbtable", "(SELECT * FROM sweetp_kpex_context WHERE id='" + ContextID + "') context")
            .load()
          AppConfig.DatabaseContextTitles = AppConfig.DatabaseContextTitles +(TestID->context.collect()(0).getString(4))
          AppConfig.DatabaseContextURLs = AppConfig.DatabaseContextURLs +(TestID->context.collect()(0).getString(3))
          val theinputString=context.collect()(0).getString(5)
          CurrentCorpus.addTestContext(TestID,theinputString)
          val realkeywordstext = context.collect()(0).getString(7)
          val theRealKeyPhrases=realkeywordstext.split(",")
          theRealKeyPhrases.foreach(phrase=>CurrentCorpus.Tests(TestID).addGoldPhrase(phrase))
//          RealKeyPhrases=RealKeyPhrases+(TestID->theRealKeyPhrases)
          LoadWordVectors(spark,TestID)
        })
      CurrentCorpus.commitChanges()
//      val AllNounPhrasesData=new textBlobAdapter(this).GetTotalNounPhrases(spark,TotalInputString)
//      val AllNounPhrasesData=new nltkAdapter(this).GetTotalNounPhrases(spark,CurrentCorpus.TotalText)
//      AllNounPhrases=AllNounPhrasesData.head
//      AllNounPhrasePosTags=AllNounPhrasesData
      AppConfig.SingleOutput = true

    }
  }

  private def LoadWordVectors(spark: SparkSession,TestID:Int): Unit =
  {
    val nlp=new NLPTools(this)
    var inputStringWords = CurrentCorpus.Tests(TestID).Words
    var AllWords="'unknown'"
    inputStringWords=inputStringWords.map(word=>
    {
      val normalWord=nlp.replaceExtraCharactersFromWord(word).trim.toLowerCase()  //Normalizing All Words
      SweetOut.printLine("Word " + word+" made "+normalWord,1)
      normalWord
    })
    inputStringWords.foreach(word => {
      AllWords=AllWords+",'"+word+"'"
    })
    SweetOut.printLine("Words:"+AllWords,1)
    val wordVectors = spark.read
      .format("jdbc")
      .option("url", MysqlConfigs.getProperty("url"))
      .option("user", MysqlConfigs.getProperty("user"))
      .option("password", MysqlConfigs.getProperty("password"))
//      .option("dbtable", "(SELECT * FROM sweetp_kpex_wordvector WHERE trim(word) IN (" + AllWords + ") limit 0,"+(inputStringWords.length+1)+") wv")
      .option("dbtable", "(SELECT * FROM sweetp_kpex_wordvectorsenna WHERE trim(word) IN (" + AllWords + ") limit 0,"+(inputStringWords.length+1)+") wv")
      .load().collect()
    inputStringWords.foreach(word => {
      // Removed At 97/06/17 val normalWord = TextMan.replace(word, Seq(), false)
      val wordVector=wordVectors.filter(row=>row.getString(4).equals(word)).take(1)
      val wordLemma = nlp.GetNormalizedAndLemmatizedWord(word,currentTestID)

      if (wordVector.length > 0 && wordLemma.length > 0) {
        SweetOut.printLine("Word Is " + word+" made "+wordLemma+" And Has Vector",1)
        addVectorFromVectorString(wordVector(0).getString(5),wordLemma,TestID)
      }
      else {
//        if(LemmatizationMaps(TestID).keySet.exists(key=>key.equals(word))) {
//
//          val wordFullLemma = LemmatizationMaps(TestID)(word)
//          if (!wordFullLemma.equals(word) && wordFullLemma.length>0) {
//            SweetOut.printLine("Now Testing " + wordFullLemma + " instead of " + word, 1)
//            val wordVector2 = spark.read
//              .format("jdbc")
//              .option("url", MysqlConfigs.getProperty("url"))
//              .option("user", MysqlConfigs.getProperty("user"))
//              .option("password", MysqlConfigs.getProperty("password"))
//              .option("dbtable", "(SELECT * FROM sweetp_kpex_wordvectorsenna WHERE trim(word) = '" + wordFullLemma + "' limit 0,1) wv")
//              .load().collect()
//            if (wordVector2.nonEmpty) {
//
//              SweetOut.printLine("Now It Is NonEmpty ", 1)
//              addVectorFromVectorString(wordVector2(0).getString(5), wordLemma, TestID)
//            }
//            else
//              SweetOut.printLine("Not Found Vector", 1)
//          }
//          else
//            SweetOut.printLine("Word Is "+word+" made "+wordLemma+"  And Has No Vector",1)
//        }
//        else
            SweetOut.printLine("No Lemma For Word",1)

      }

    })
  }
  private def addVectorFromVectorString(VectorString:String,word:String,TestID:Int): Unit =
  {
    val StringVector=VectorString.split(",")
    val IntVector:Array[Double]=StringVector.map(sv=>sv.toDouble)
    TotalWordEmbed.PutWordVector(TestID,word,IntVector)
  }

  override protected def Init(spark: SparkSession, args: Array[String]): Unit = {
    super.Init(spark, args)

  }
  override protected def LoadPerTestArgs(spark: SparkSession,TestID:Int): Unit =
  {
    AppConfig.ResultDirectoryName="result"+TestID
    super.LoadPerTestArgs(spark,TestID)
    AppConfig.DataSetKeyWordsPath = AppConfig.ResultDirectory + "/keywords" + TestID + ".txt"
  }


}
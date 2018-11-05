package ir.sweetsoft.kpex

import java.util.Properties

import ir.sweetsoft.WordEmbedding.WordEmbed
import ir.sweetsoft.common.SweetOut
import ir.sweetsoft.nlp.{NLPTools, textBlobAdapter}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

@SerialVersionUID(1145L)
class MySQLKpexEngine extends KpexEngine  {

//  private var inputString = ""
//  private var ContextID = 0

  override protected def LoadArgs(spark: SparkSession, args: Array[String]): Unit = {
    if (args.length > 2) {
      AppConfig.DatabaseTestIDsString=args(2)
//      AppConfig.DatabaseTestIDs = AppConfig.DatabaseTestIDsString.split(",").map(id=>id.toInt).toSeq

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

//      SweetOut.printLine(AppConfig.DatabaseTestID)
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

      var TotalInputString=""
      testCollected.foreach(test=>
        {
          var ContextID = test.getInt(11)
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
          var NormalizedString=NormalizeString(theinputString)
          inputString=inputString+(TestID->NormalizedString)
          if(!NormalizedString.substring(NormalizedString.length-1).equals("."))
            NormalizedString = NormalizedString+"."
          NormalizedString=NormalizedString.trim
          if(TotalInputString!="")
            TotalInputString=TotalInputString+"\r\n"
          TotalInputString=TotalInputString+AppConfig.TEST_SEPARATOR_TEXT+"\r\n"+NormalizedString
          var realkeywordstext = context.collect()(0).getString(7)
          val nlp=new NLPTools(this)
          nlp.addToLemmatizationMap(inputString(TestID),TestID)
          var theRealKeyPhrases=realkeywordstext.split(",")
          theRealKeyPhrases=theRealKeyPhrases.map(Phrase=>
          {
            nlp.addToLemmatizationMap(Phrase,TestID)
            val Words=nlp.GetStringWords(Phrase)
            var Result=""
            Words.foreach(Word=>
            {
              Result=Result+" "+nlp.GetNormalizedAndLemmatizedWord(Word,currentTestID)
            })
            Result.trim.toLowerCase
          })
          RealKeyPhrases=RealKeyPhrases+(TestID->theRealKeyPhrases)
          LoadWordVectors(spark,TestID)
        })

      TotalInputString=TotalInputString+"\r\n"+AppConfig.TEST_SEPARATOR_TEXT+"\r\n"
      AllNounPhrases=new textBlobAdapter(this).GetTotalNounPhrases(spark,TotalInputString)
      AppConfig.SingleOutput = true

    }
  }

  private def LoadWordVectors(spark: SparkSession,TestID:Int): Unit =
  {
    //      inputString.split(Array(' ','\n','.',',',':',';'))
    val nlp=new NLPTools(this)
    var inputStringWords = nlp.GetStringWords(inputString(TestID))
//    SweetOut.printLine("Words Are:")
//    inputStringWords.foreach(Word=>
//    {
//      SweetOut.printLine(Word)
//    })
    var WordsVectorMap: Map[String, Array[Double]] = Map()
    var AllWords="'unknown'"
    inputStringWords=inputStringWords.distinct.map(word=>
    {
      val normalWord=nlp.replaceExtraCharactersFromWord(word).trim  //Normalizing All Words
      SweetOut.printLine("Word " + word+" made "+normalWord,1)
      normalWord
    })
    inputStringWords.foreach(word => {
      AllWords=AllWords+",'"+word+"'"
    })
    val wordVectors = spark.read
      .format("jdbc")
      .option("url", MysqlConfigs.getProperty("url"))
      .option("user", MysqlConfigs.getProperty("user"))
      .option("password", MysqlConfigs.getProperty("password"))
      .option("dbtable", "(SELECT * FROM sweetp_kpex_wordvector WHERE trim(word) IN (" + AllWords + ") limit 0,"+(inputStringWords.length+1)+") wv")
      .load().collect()
    inputStringWords.foreach(word => {
      // Removed At 96/06/17 val normalWord = TextMan.replace(word, Seq(), false)
      val wordVector=wordVectors.filter(row=>row.getString(4).equals(word)).take(1)
      val wordLemma = nlp.GetNormalizedAndLemmatizedWord(word,currentTestID)
      if (wordVector.length > 0 && wordLemma.length > 0) {
        SweetOut.printLine("Word Is " + word+" made "+wordLemma+" And Has Vector",1)
          val StringVector=wordVector(0).getString(5).split(",")
          val IntVector:Array[Double]=StringVector.map(sv=>sv.toDouble)
          WordsVectorMap = WordsVectorMap + (wordLemma -> IntVector)
      }
      else {
        SweetOut.printLine("Word Is "+word+" And Has No Vector",1)
      }

    })
    wordEmbeds=wordEmbeds+(TestID->new WordEmbed(WordsVectorMap))
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
  override def getInputStringRDD(spark: SparkSession): RDD[String] = {
    val lineArray = inputString(currentTestID).split("\n")
    val input = spark.sparkContext.parallelize(lineArray)
    input
  }


}
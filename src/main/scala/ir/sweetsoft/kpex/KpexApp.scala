package ir.sweetsoft.kpex

import ir.sweetsoft.common.SweetOut
import org.apache.spark.sql.SparkSession

object KpexApp extends MySQLKpexEngine {
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .master("local")
      .appName("SweetFirstGraphx")
      .config("spark.sql.warehouse.directory", "/in/")
      .getOrCreate()
    Init(spark,args)

    AppConfig.DatabaseTestIDs.foreach(TestID=>{
      resetContextData()
      currentTestID=TestID
      LoadPerTestArgs(spark,TestID)
      val inputStringRDD=getInputStringRDD(spark)

      var inputString = ""
      inputStringRDD.collect().foreach {
        s => inputString += s
      }
      //    SweetOut.printLine("After Init3")
      val theGraphIO:GraphIO=new GraphIO(this)
      val theIO:IO=new IO(this)
//      NounPhrases=new textBlobAdapter(this).GetNounPhrases(spark,inputString,currentTestID)
      NounPhrasePosTags=AllNounPhrasePosTags(currentTestID)
      NounPhrases=NounPhrasePosTags.keySet.toSeq
//      NounPhrases.foreach(np=>{
//        if(NounPhrases.exists(np2 => !np.equals(np2) && np2.contains(np)))
//          {
//            NounPhrasePosTags = NounPhrasePosTags - np
//          }
//      })
      NounPhrases=NounPhrasePosTags.keySet.toSeq
      //    RemoveExtraWordsFromNounPhrasesBySimilarity()
      theGraphIO.makeWordGraphFile(spark,inputString)
      val file = spark.sparkContext.textFile(AppConfig.GraphPath)

      var graph = theGraphIO.LoadGraph(spark,file,wordEmbeds(TestID))
      theGraphIO.MakeVisualizedGraphFile(spark,graph)
      var IdentificationMapText=""
      NewIdentificationMap.keys.foreach{
        Key=>
          IdentificationMapText+=Key+"\t"+NewIdentificationMap(Key)
      }
      theIO.WriteToFile(spark,AppConfig.IdentificationMapPath,IdentificationMapText,false)
      SweetOut.printLine("Graph Size:" + graph.vertices.count(),1)
      val vertexSeq = graph.vertices.map(v => v._1).collect().toSeq
      var VertexMap:Seq[(Long,Double)]=null
      if(AppConfig.GraphImportanceMethod==AppConfig.METHOD_ECCENTERICITY)
        VertexMap = SweetGraph.getEccentericity(graph, vertexSeq).collect
      else if(AppConfig.GraphImportanceMethod==AppConfig.METHOD_DEGREE)
        VertexMap = SweetGraph.getDegree(graph, vertexSeq).collect
      else if(AppConfig.GraphImportanceMethod==AppConfig.METHOD_CLOSENESS)
        VertexMap = SweetGraph.getCloseness(spark.sparkContext,graph, vertexSeq)
      else if(AppConfig.GraphImportanceMethod==AppConfig.METHOD_NE_RANK)
        VertexMap = SweetGraph.getNE_Rank(graph, vertexSeq).collect

      //    val VertexMap=SweetGraph.getEccentericityAndCloseness(graph,vertexSeq)
      PrepareLocalOutputData(spark,VertexMap,file)
      //
    })
    WriteResultsToStorage(spark)



  }


}
@SerialVersionUID(114L)
class ValueType(var weight:Double,var role:String) extends Serializable
{
  var Weight:Double=weight
  var Role:String=role
}
@SerialVersionUID(114L)
class SingleValueType(var Target:String,var weight:Double,var role:String) extends Serializable
{
  var Weight:Double=weight
  var Role:String=role
}

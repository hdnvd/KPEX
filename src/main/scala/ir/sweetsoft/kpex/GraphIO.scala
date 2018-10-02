package ir.sweetsoft.kpex


import ir.sweetsoft.WordEmbedding.WordEmbed
import ir.sweetsoft.common.SweetOut
import ir.sweetsoft.nlp.{NLPTools}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.util.hashing.MurmurHash3
class GraphIO(ApplicationContext:KpexContext) extends KpexClass(ApplicationContext) {

  var LastWords: Array[String] = Array("", "", "","","","","","","","")
  var LastWordPosTags: Array[String] = Array("unknown", "unknown", "unknown","unknown", "unknown", "unknown","unknown", "unknown", "unknown","unknown")
  var Nouns: Seq[String] = Seq()
  var Adjectives: Seq[String] = Seq()
  def makeWordGraphFile(spark: SparkSession,theInputString:String) {
    SweetOut.printLine("/Making Word Graph...",2)
    val sc = spark.sparkContext
// Removed At 96/06/17    var inputString = TextMan.replace(theInputString, Seq(",", "."), false)
    var inputString=theInputString
    val inputSentences = inputString.split("\\.")
    val inputSentencesRDD = sc.parallelize(inputSentences)
    SweetOut.printLine("Lemmatizing Sentences...",1)
    val nlp=new NLPTools(appContext)
    val lemmatized = inputSentencesRDD.map(nlp.plainTextToNormalLemmas(_, appContext.AppConfig.hasPosTagging))
    //    val words2 = lemmatized.flatMap(f => f)
//    SweetOut.printLine("Sentence Count:" + lemmatized.count)
    var counts: RDD[(String, ValueType)] = null
    if (appContext.AppConfig.hasPosTagging)
      counts = lemmatized.flatMap((wordArray) => FlatMapMethodWithPosTagging(wordArray)).reduceByKey { case (x, y) =>
        val xRoles = x.Role.toLowerCase.split("\t")
        val yRoles = x.Role.toLowerCase.split("\t")
        val xRoleFrom = xRoles(0).trim
        val xRoleTo = xRoles(1).trim
        val yRoleFrom = yRoles(0).trim
        val yRoleTo = yRoles(1).trim
        val RoleFrom = {
          if (xRoleFrom.contains("nn")) {
            xRoleFrom
          }
          else if (yRoleFrom.contains("nn"))
            yRoleFrom
          else if (xRoleFrom.contains("jj"))
            xRoleFrom
          else
            yRoleFrom
        }
        val RoleTo = {
          if (xRoleTo.contains("nn"))
            xRoleTo
          else if (yRoleTo.contains("nn"))
            yRoleTo
          else if (xRoleTo.contains("jj"))
            xRoleTo
          else
            yRoleTo
        }
        new ValueType(x.weight + y.weight, RoleFrom + "\t" + RoleTo)
      }
    makeWordGraphFileFromData(spark, counts)
    SweetOut.printLine("/Word Graph Making Completed",2)

  }
  def makeWordGraphFileFromData(spark: SparkSession, counts: RDD[(String, ValueType)]): Unit = {

    val theIO:IO=new IO(appContext)
    if (appContext.AppConfig.StorageType == appContext.AppConfig.FILE_MODE) {

      SweetOut.printLine("Writing In " + appContext.AppConfig.GraphPath,1)
      if (scala.tools.nsc.io.Path(appContext.AppConfig.GraphPath).exists)
        scala.tools.nsc.io.Path(appContext.AppConfig.GraphPath).delete()
      for (x <- counts) {
        theIO.WriteToFile(spark, appContext.AppConfig.GraphPath, x._1 + "\t" + x._2.weight + "\t" + x._2.Role + "\r\n", true)
      }

    }
    else {
      var Out: String = ""

      var Res: Seq[String] = Seq()
      val cnt = counts.collect
      for (x <- cnt) {
        Out += x._1 + "\t" + x._2.weight + "\t" + x._2.Role + "\r\n"
      }
      theIO.WriteToFile(spark, appContext.AppConfig.GraphPath, Out, false)
    }
  }
  def FlatMapMethodWithPosTagging(words: Array[String]): Seq[(String, ValueType)] = {
    var result = Seq[(String, ValueType)]()
    var i=0
    for(i<-appContext.AppConfig.WindowSize-1 to 0 by -1)
    {
      LastWords(i) ="unknown"
      LastWordPosTags(i) = "notsetyet"
    }

    val nlp=new NLPTools(appContext)
    words.foreach {
      word =>
//        SweetOut.printLine("Word is "+word,1)
        val wordParts = word.split("_")
        if(wordParts.length>=2)
          {
            val wordPOSTag = wordParts(0)
            var PureWord = wordParts(1)
            if (nlp.isStopWord(PureWord.trim) || nlp.replaceExtraCharactersFromWord(PureWord).trim.length<2) {}
            else {
              if (wordPOSTag.toLowerCase.contains("nn"))
                Nouns = Nouns :+ (PureWord)
              if (wordPOSTag.toLowerCase.contains("jj"))
                Adjectives = Adjectives :+ (PureWord)
              pushToWordList(word)
              UpdateWordFrequency(word)
              result = result ++ getWindowWordsEdges
          }

        }
    }

    result
  }

  def UpdateWordFrequency(Word:String): Unit =
  {
    val theWord=Word.trim.toLowerCase
    val theWordParts = theWord.split("_")
    val theWordPOSTag = theWordParts(0)
    val theWord_Pure = theWordParts(1)
    if(appContext.WordFrequencies.keys.exists(_==theWord_Pure))
      {
        appContext.WordFrequencies=appContext.WordFrequencies+(theWord_Pure->(appContext.WordFrequencies(theWord_Pure)+1))
      }
    else
      appContext.WordFrequencies=appContext.WordFrequencies+(theWord_Pure->1)
  }

  def LoadGraph(spark: SparkSession,file:RDD[String],wordEmbed: WordEmbed): Graph[Int,Double] =
  {
    var ValueIndex=2
    var edgesRDD: RDD[Edge[Double]] = file.map(line => line.split("\t"))
      .flatMap {
        case line: Array[String] =>
          var Influence=1.0d
          var OutInfluence=1.0d
          var role=0

          val FirstWord=line(0).toString.trim.toLowerCase
          val SecondWord=line(1).toString.trim.toLowerCase

          val CoOccurance=line(ValueIndex).toDouble
          val FirstWordFrequency=appContext.WordFrequencies(FirstWord)
          val SecondWordFrequency=appContext.WordFrequencies(SecondWord)
          if(Nouns.contains(FirstWord)){
            OutInfluence=appContext.AppConfig.NounOutInfluence
            role=1
          }
          else if(Adjectives.contains(FirstWord)){
            OutInfluence=appContext.AppConfig.AdjectiveOutInfluence
            role=2
          }
          if(Nouns.contains(SecondWord)){
            Influence=appContext.AppConfig.NounInfluence
            role=1
          }
          else if(Adjectives.contains(SecondWord)){
            Influence=appContext.AppConfig.NounInfluence
            role=2
          }

          var SimilarityInfluence=1d
          if(appContext.AppConfig.HasSimilarityEdgeWeighting)
            {
              val Distance=wordEmbed.getEuclideanDistanceBetweenWords(FirstWord,SecondWord)
              if(Distance== -2d)
              {
                SweetOut.printLine("Distance Between "+FirstWord+" and "+SecondWord+" is not defined",1)
              }
              else
                {
                  SimilarityInfluence=FirstWordFrequency*SecondWordFrequency/math.pow(Distance,2)
                }

            }
          val DiceResult=Dice(FirstWordFrequency,SecondWordFrequency,CoOccurance)
          appContext.NewIdentificationMap=appContext.NewIdentificationMap+(MurmurHash3.stringHash(FirstWord).toLong->FirstWord)
          appContext.NewIdentificationMap=appContext.NewIdentificationMap+(MurmurHash3.stringHash(SecondWord).toLong->SecondWord)
          var StraightWeight:Double=0.0d
          var ReverseWeight:Double=0.0d
          SweetOut.printLine("SimilarityInfluence is "+SimilarityInfluence,1)
          if(appContext.AppConfig.GraphImportanceMethod==appContext.AppConfig.METHOD_NE_RANK)
            {
              StraightWeight=(OutInfluence*100*DiceResult)*SimilarityInfluence
              ReverseWeight=(Influence*100*DiceResult)*SimilarityInfluence
            }
            else
            {
              StraightWeight=(OutInfluence*100/DiceResult)/SimilarityInfluence
              ReverseWeight=(Influence*100/DiceResult)/SimilarityInfluence
            }
          if(ReverseWeight.toString.trim.toLowerCase.equals("nan"))
          {
            SweetOut.printLine("Edge is NaN inf:"+OutInfluence+" InitailWeight:"+DiceResult+ " SimilarityInfluence:"+SimilarityInfluence,1)
          }
          var ResultEdges=Seq(Edge[Double](MurmurHash3.stringHash(FirstWord), MurmurHash3.stringHash(SecondWord),StraightWeight))
          ResultEdges=ResultEdges:+Edge[Double](MurmurHash3.stringHash(SecondWord), MurmurHash3.stringHash(FirstWord), ReverseWeight)
          if(SimilarityInfluence>=appContext.AppConfig.SimilarityMinThreshold)
            appContext.ExistentSimilarEdges=appContext.ExistentSimilarEdges+((MurmurHash3.stringHash(FirstWord).toLong,MurmurHash3.stringHash(SecondWord).toLong)->1)
          ResultEdges
      }

    var graph = Graph.fromEdges(edgesRDD, 1)
//    AddHelperEdges(spark,graph,wordEmbed,edgesRDD)
    graph
  }

  private def AddHelperEdges(spark: SparkSession,theGraph: Graph[Int,Double],wordEmbed: WordEmbed,theEdgesRDD:RDD[Edge[Double]]): Graph[Int,Double] =
  {
    var graph=theGraph
    var edgesRDD=theEdgesRDD
    val Nodes=graph.vertices.collect
    var NewEdges:Seq[Edge[Double]]=Seq()
    Nodes.foreach(FirstWord=>
    {
      val theFirstWord=appContext.NewIdentificationMap(FirstWord._1)
      Nodes.foreach(SecondWord=>{
        val theSecondWord=appContext.NewIdentificationMap(SecondWord._1)
        val Similarity=wordEmbed.getSimilarityBetweenWords(theFirstWord,theSecondWord)
        if(Similarity>=appContext.AppConfig.SimilarityMinThreshold)
        {
          val Key=(FirstWord._2.toLong,SecondWord._2.toLong)
          if(!appContext.ExistentSimilarEdges.keySet.exists(a=>(a._1==FirstWord._1.toLong) && (a._2==SecondWord._1.toLong) || (a._1==SecondWord._1.toLong) && (a._2==FirstWord._1.toLong)))
          {
            //                  SweetOut.printLine("EveryThing is OK")
          }
          else
          {

            SweetOut.printLine("Adding Helper Edges",1)
            NewEdges=NewEdges:+Edge[Double](FirstWord._1.toLong, SecondWord._1.toLong,Similarity*30)
          }
        }
      })



    })
    if(NewEdges.nonEmpty)
    {

      edgesRDD=edgesRDD.union(spark.sparkContext.parallelize(NewEdges))
      graph=Graph.fromEdges(edgesRDD, 1)
      return graph
    }
    else
      return theGraph

  }
  def MakeVisualizedGraphFile(spark: SparkSession,graph:Graph[Int,Double]): Unit =
  {
    var VisualGraphContent:String="<html>\n<head>\n<meta http-equiv=\"content -type\" content=\"text/html; charset=UTF-8\">\n<meta charset =\"utf-8\"/>\n<script type = \"text/javascript\" src = \"raphael-min.js\"></script>\n<script type = \"text/javascript\" src = \"dracula_graffle.js\"></script>\n<script type = \"text/javascript\" src = \"dracula_graph.js\"></script>\n<link href=\"style.css\" rel=\"stylesheet\" /></head>\n<body>\n<script type=\"text/javascript\">\n var width=1300;\n var height=1500;\nwindow.onload = function() {\n\t\tvar MainGraph = new Graph();"

    val Triplets=graph.triplets.collect.sortBy(Triplet=>Triplet.srcId)
    Triplets.foreach(Triplet=>
    {
      if(!Triplets.exists(TempTriplet=>TempTriplet.srcId<Triplet.srcId && TempTriplet.srcId==Triplet.dstId && TempTriplet.dstId==Triplet.srcId))
        VisualGraphContent+="MainGraph.addEdge(\""+appContext.NewIdentificationMap(Triplet.srcId)+"\",\""+appContext.NewIdentificationMap(Triplet.dstId)+"\", { directed: false , label:\""+ Triplet.attr.toString.substring(0,Math.min(5,Triplet.attr.toString.length))+"\"});\r\n"
    })
    VisualGraphContent+="var MainGraphlayouter = new Graph.Layout.Spring(MainGraph);\n\t\tMainGraphlayouter.layout();\n\t\tvar MainGraphrenderer = new Graph.Renderer.Raphael('MainGraphcanvas', MainGraph, width, height);\n\t\tMainGraphrenderer.draw();\n\t\t};\n\t\t</script>\n<div class='canvas'>\n<div class='canvasTitle'>Word Graph</div>\n<div id=\"MainGraphcanvas\" class='canvasData'></div>\n</div>\n</body>\n</html>\r\n"
    if (appContext.AppConfig.StorageType == appContext.AppConfig.FILE_MODE)
      {
          SweetOut.printLine("Error: MakeVisualizedGraphFile is not implemented for file mode",10)
      }
    else
      {
        val theIO:IO=new IO(appContext)
        theIO.WriteToFile(spark, appContext.AppConfig.VisualGraphPath, VisualGraphContent, false)
      }
  }
  def Dice(FirstWordFreq:Double,SecondWordFreq:Double,CoOccurance:Double):Double=
  {
    val Result=2*CoOccurance/(FirstWordFreq+SecondWordFreq)
    Result
  }
  def pushToWordList(word: String): Unit = {
    var i=0
    for(i<-appContext.AppConfig.WindowSize-1 to 1 by -1)
      {

        LastWords(i) = LastWords(i-1)
        LastWordPosTags(i) = LastWordPosTags(i-1)
      }
    LastWords(0) = word
    LastWordPosTags(0) = "notsetyet" // To Set From FlatMap
  }
  def getWindowWordsEdges: Seq[(String, ValueType)] = {
    var result = Seq[(String, ValueType)]()
    var i=0
    val theLastWord = LastWords(0)
    val theLastWordParts = theLastWord.split("_")
    val theLastWordPOSTag = theLastWordParts(0)
    val theLastWord_Pure = theLastWordParts(1)

    for(i<-appContext.AppConfig.WindowSize-1 to 1 by -1)
    {
      val CurrentWord = LastWords(i)
      if (!CurrentWord.equals("unknown"))
        {
          val CurrentWordParts = CurrentWord.split("_")
          val CurrentWordPOSTag = CurrentWordParts(0)
          val CurrentWord_Pure = CurrentWordParts(1)
          result = result :+ (CurrentWord_Pure.trim + "\t" + theLastWord_Pure.trim, new ValueType(1d, LastWordPosTags(1) + "\t" + theLastWordPOSTag))
        }

    }
    LastWordPosTags(0) = theLastWordPOSTag
    result
  }

}

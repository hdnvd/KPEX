package ir.sweetsoft.WordEmbedding

import ir.sweetsoft.common.SweetOut


class WordEmbed extends Serializable{


  private var TotalWordsVectorMap:Map[Int,Map[String, Array[Double]]]=Map()
  private var TotalCorpusWordCount:Int=0//Not Considering Distinct Words
  private var TotalTestWordCounts:Map[Int,Int] = Map()//Not Considering Distinct Words
  private var CorpusVectorsSum:Array[Double] = Array()
  private var CorpusVectorsMean:Array[Double] = Array()
  private var TestVectorsSums:Map[Int,Array[Double]] = Map()
  private var TestVectorsMeans:Map[Int,Array[Double]] = Map()


  private var TotalCorpusNounPhraseWordCount:Int=0//Not Considering Distinct Words
  private var TotalTestNounPhraseWordCounts:Map[Int,Int] = Map()//Not Considering Distinct Words
  private var CorpusNounPhraseVectorsSum:Array[Double] = Array()
  private var CorpusNounPhraseVectorsMean:Array[Double] = Array()
  private var TestNounPhraseVectorsSums:Map[Int,Array[Double]] = Map()
  private var TestNounPhraseVectorsMeans:Map[Int,Array[Double]] = Map()

  private[this] var _TestID: Int = -1

  def TestID: Int = _TestID

  def TestID_=(value: Int): Unit = {
    _TestID = value
  }

  private def CurrentWordVectorMap: Map[String, Array[Double]] =
  {
    if(TotalWordsVectorMap.exists(a=>a._1==TestID))
      TotalWordsVectorMap(TestID)
    else
      null
  }
  private def increaseWordCount(TestID:Int): Unit =
  {
    if(TotalTestWordCounts.exists(a=>a._1==TestID))
      TotalTestWordCounts=TotalTestWordCounts + (TestID-> (TotalTestWordCounts(TestID) + 1))
    else
      TotalTestWordCounts=TotalTestWordCounts + (TestID-> 1)
    TotalCorpusWordCount=TotalCorpusWordCount+1
  }
  private def increaseNounPhraseWordCount(TestID:Int): Unit =
  {
    if(TotalTestNounPhraseWordCounts.exists(a=>a._1==TestID))
      TotalTestNounPhraseWordCounts=TotalTestNounPhraseWordCounts + (TestID-> (TotalTestNounPhraseWordCounts(TestID) + 1))
    else
      TotalTestNounPhraseWordCounts=TotalTestNounPhraseWordCounts + (TestID-> 1)
    TotalCorpusNounPhraseWordCount=TotalCorpusNounPhraseWordCount+1
  }
  private def AddWordVectorToVectorsSum(TestID:Int,Vector:Array[Double]): Unit =
  {
    val ArrayLength=Vector.length
    var TestVectorsSum:Array[Double]= null
    if(TestVectorsSums.exists(a=>a._1==TestID))
      TestVectorsSum=TestVectorsSums(TestID)
    else
      TestVectorsSum=new Array[Double](ArrayLength)

    if(CorpusVectorsSum.length==0)
        CorpusVectorsSum=new Array[Double](ArrayLength)

    require(ArrayLength==CorpusVectorsSum.length && ArrayLength==TestVectorsSum.length)
    CorpusVectorsSum=ArraySum(CorpusVectorsSum,Vector)
    TestVectorsSum=ArraySum(TestVectorsSum,Vector)
//    for(i<-Range(0,ArrayLength))
//      {
//        val VectorItem=Vector(i)
//        CorpusVectorsSum(i)=CorpusVectorsSum(i)+VectorItem
//        TestVectorsSum(i)=TestVectorsSum(i)+VectorItem
//      }
    TestVectorsSums=TestVectorsSums+(TestID->TestVectorsSum)
  }
  private def AddWordVectorToNounPhraseVectorsSum(TestID:Int,Vector:Array[Double]): Unit =
  {
    val ArrayLength=Vector.length
    var TestNounPhraseVectorsSum:Array[Double]= null
    if(TestNounPhraseVectorsSums.exists(a=>a._1==TestID))
      TestNounPhraseVectorsSum=TestNounPhraseVectorsSums(TestID)
    else
      TestNounPhraseVectorsSum=new Array[Double](ArrayLength)

    if(CorpusNounPhraseVectorsSum.length==0)
      CorpusNounPhraseVectorsSum=new Array[Double](ArrayLength)

    require(ArrayLength==CorpusNounPhraseVectorsSum.length && ArrayLength==TestNounPhraseVectorsSum.length)
    CorpusNounPhraseVectorsSum=ArraySum(CorpusNounPhraseVectorsSum,Vector)
    TestNounPhraseVectorsSum=ArraySum(TestNounPhraseVectorsSum,Vector)
    TestNounPhraseVectorsSums=TestNounPhraseVectorsSums+(TestID->TestNounPhraseVectorsSum)
  }
  private def TestWordVectorMap(theTestID:Int): Map[String, Array[Double]] =
  {
    if(TotalWordsVectorMap.exists(a=>a._1==theTestID))
      TotalWordsVectorMap(theTestID)
    else
      null
  }
  private def isWordVectorExists(Word:String,WordsVectorMap:Map[String, Array[Double]]): Boolean =
  {
    if(WordsVectorMap.contains(Word) && WordsVectorMap(Word).nonEmpty && WordsVectorMap(Word).nonEmpty)
      true
    else
      false
  }

  private def isWordVectorExists(Word:String): Boolean =
  {
    isWordVectorExists(Word,CurrentWordVectorMap)
  }


  private def getEuclideanDistanceBetweenWordAndVector(Word:String,Vector:Array[Double]): Double =
  {
    val WordsVectorMap:Map[String, Array[Double]]=CurrentWordVectorMap
    if(isWordVectorExists(Word))
    {
      val Vector1 = WordsVectorMap(Word)
      val distance = EuclideanDistance.getEuclideanDistance(Vector1, Vector)
//      SweetOut.printLine("Distance Is "+distance,1)
      distance
    }
    else
      -2d
  }
  private def ArraySum(Array1:Array[Double],Array2:Array[Double]): Array[Double] =
  {
    require(Array1.length==Array2.length)
    val len=Array1.length
    var SumArray=new Array[Double](len)
    for(i<- Range(0,len))
      {
        SumArray(i)=Array1(i)+Array2(i)
      }
    SumArray
  }
  private def DivideVectorToNumber(Array:Array[Double],DevidingNumber: Double): Array[Double] =
  {
    val len=Array.length
    var ResultArray=new Array[Double](len)
    for(i<- Range(0,len))
    {
      ResultArray(i)=Array(i)/DevidingNumber
    }
    ResultArray
  }
  private def getVectorsMeanVector(Vectors:Seq[Array[Double]]):Array[Double]=
  {
    val VectorCount=Vectors.size
    require(VectorCount>0)
    val len=Vectors(0).length
    var sumVector=new Array[Double](len)
    Vectors.foreach(Vect=>sumVector=ArraySum(Vect,sumVector))
    DivideVectorToNumber(sumVector,VectorCount)
  }
  private def getMeanVector(Words:Seq[String]): Array[Double] =
  {
    var CurrentWVM=CurrentWordVectorMap
    var VectorSeq:Seq[Array[Double]]=Seq()
    Words.foreach(Word=>{
      if(isWordVectorExists(Word))
        VectorSeq=VectorSeq:+CurrentWVM(Word)
    })
    if(VectorSeq.isEmpty)
      return null
    val meanVector:Array[Double]=getVectorsMeanVector(VectorSeq)
    meanVector
  }
  def PutWordVector(theTestID:Int,Word:String,vectorOfTheWord: Array[Double]): Unit =
  {
    var WordVectorMap:Map[String, Array[Double]]=TestWordVectorMap(theTestID)
    if(WordVectorMap==null)
      WordVectorMap=Map()
//    var MeanVectorOfTheWord:Array[Double]=null
//    if(WordVectorMap.keySet.exists(key=>key.equals(Word)))
//      MeanVectorOfTheWord=getVectorsMeanVector(Seq(vectorOfTheWord,WordVectorMap(Word)))
//    else
//      MeanVectorOfTheWord=vectorOfTheWord
    WordVectorMap=WordVectorMap + (Word -> vectorOfTheWord)
    TotalWordsVectorMap=TotalWordsVectorMap+(theTestID->WordVectorMap)
    increaseWordCount(theTestID)
    AddWordVectorToVectorsSum(theTestID,vectorOfTheWord)
  }
  def addNounPhraseWord(theTestID:Int,Word:String): Unit =
  {
    require(TotalWordsVectorMap.keySet.contains(theTestID))
    if(TotalWordsVectorMap(theTestID).keySet.contains(Word))//Some Words Has No Vector
      {
        SweetOut.printLine("WOORD "+Word,1)
        increaseNounPhraseWordCount(theTestID)
        AddWordVectorToNounPhraseVectorsSum(theTestID,TotalWordsVectorMap(theTestID)(Word))
      }
    else
      SweetOut.printLine("W++RD "+Word,1)

  }

  private def commitNounPhraseChanges(): Unit =
  {
    val ArrayLength=CorpusNounPhraseVectorsSum.length
    CorpusNounPhraseVectorsMean=new Array[Double](ArrayLength)
    for(i<-Range(0,ArrayLength))
    {
      CorpusNounPhraseVectorsMean(i)=CorpusNounPhraseVectorsSum(i)/TotalCorpusNounPhraseWordCount
    }
    TotalTestNounPhraseWordCounts.foreach(TestVectorCount=>{
      val theTestID=TestVectorCount._1
      val Count=TestVectorCount._2
      var TestNounPhraseVectorsMean:Array[Double]=new Array[Double](ArrayLength)
      val TestNounPhraseVectorSum=TestNounPhraseVectorsSums(theTestID)
      for(i<-Range(0,ArrayLength))
      {
        TestNounPhraseVectorsMean(i)=TestNounPhraseVectorSum(i)/Count
      }
      TestNounPhraseVectorsMeans=TestNounPhraseVectorsMeans + (theTestID->TestNounPhraseVectorsMean)
    })
  }
  def commitChanges(): Unit =
  {
    val ArrayLength=CorpusVectorsSum.length
    CorpusVectorsMean=new Array[Double](ArrayLength)
    for(i<-Range(0,ArrayLength))
    {
      CorpusVectorsMean(i)=CorpusVectorsSum(i)/TotalCorpusWordCount
    }


    TotalTestWordCounts.foreach(TestVectorCount=>{
      val theTestID=TestVectorCount._1
      val Count=TestVectorCount._2
      var TestVectorsMean:Array[Double]=new Array[Double](ArrayLength)
      val TestVectorSum=TestVectorsSums(theTestID)
//      SweetOut.printLine("TestVectorSum:",1)
      for(i<-Range(0,ArrayLength))
      {

//        SweetOut.printOne(TestVectorSum(i)+"+",1)
        TestVectorsMean(i)=TestVectorSum(i)/Count
//        SweetOut.printOne(TestVectorsMean(i)+",",1)
      }
      TestVectorsMeans=TestVectorsMeans + (theTestID->TestVectorsMean)
    })
    commitNounPhraseChanges()
  }
  def getSimilarityBetweenWords(Word1:String,Word2:String): Double =
  {
    if(isWordVectorExists(Word1) && isWordVectorExists(Word2))
    {
      val WordsVectorMap:Map[String, Array[Double]]=CurrentWordVectorMap
      val Vector1 = WordsVectorMap(Word1)
      val Vector2 = WordsVectorMap(Word2)
      var similarity = CosineSimilarity.cosineSimilarity(Vector1, Vector2)
      similarity = similarity + 1 //Normalizing Similarity
      similarity
    }
    else
      -2d
  }
  def getEuclideanDistanceBetweenWords(Word1:String,Word2:String,TestID:Int): Double =
  {

    getEuclideanDistanceBetweenWords(Word1,Word2,TotalWordsVectorMap(TestID))
  }
  def getEuclideanDistanceBetweenWords(Word1:String,Word2:String): Double =
  {
    getEuclideanDistanceBetweenWords(Word1,Word2,CurrentWordVectorMap)
  }
  private def getEuclideanDistanceBetweenWords(Word1:String,Word2:String,WordsVectorMap:Map[String, Array[Double]]): Double =
  {
    if(isWordVectorExists(Word1,WordsVectorMap) && isWordVectorExists(Word2,WordsVectorMap))
    {
      val Vector1 = WordsVectorMap(Word1)
      val Vector2 = WordsVectorMap(Word2)
      val distance = EuclideanDistance.getEuclideanDistance(Vector1, Vector2)
      distance
    }
    else
      -2d
  }
  def getEuclideanDistanceFromCorpus(Word:String): Double =
  {
    getEuclideanDistanceBetweenWordAndVector(Word,CorpusVectorsMean)
  }
  def getEuclideanDistanceFromCurrentTest(Word:String): Double =
  {
    getEuclideanDistanceBetweenWordAndVector(Word,TestVectorsMeans(TestID))
  }

  def getEuclideanDistanceFromCorpusNounPhrases(Word:String): Double =
  {
    getEuclideanDistanceBetweenWordAndVector(Word,CorpusNounPhraseVectorsMean)
  }
  def getEuclideanDistanceFromCurrentTestNounPhrases(Word:String): Double =
  {
    getEuclideanDistanceBetweenWordAndVector(Word,TestNounPhraseVectorsMeans(TestID))
  }
  def getEuclideanDistanceFromCorpus(Words:Seq[String]): Double =
  {
    val meanVector:Array[Double]=getMeanVector(Words)
    if(meanVector!=null)
      EuclideanDistance.getEuclideanDistance(meanVector, CorpusVectorsMean)
    else
      0d
  }

  def getEuclideanDistanceFromCurrentTest(Words:Seq[String]): Double =
  {
    val meanVector:Array[Double]=getMeanVector(Words)
    if(meanVector!=null)
      EuclideanDistance.getEuclideanDistance(meanVector, TestVectorsMeans(TestID))
    else
      0d
  }
}

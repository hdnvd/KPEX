package ir.sweetsoft.WordEmbedding


class WordEmbed(WordsVectorMap: Map[String, Array[Double]]) extends Serializable{


  def getSimilarityBetweenWords(Word1:String,Word2:String): Double =
  {
    if(isWordVectorExists(Word1) && isWordVectorExists(Word2))
      {
        val Vector1 = WordsVectorMap(Word1).tail
        val Vector2 = WordsVectorMap(Word2).tail
        var similarity = CosineSimilarity.cosineSimilarity(Vector1, Vector2)
        similarity = similarity + 1 //Normalizing Similarity
        similarity
      }
    else
      -2d
  }
  def getEuclideanDistanceBetweenWords(Word1:String,Word2:String): Double =
  {
    if(isWordVectorExists(Word1) && isWordVectorExists(Word2))
    {
      val Vector1 = WordsVectorMap(Word1).tail
      val Vector2 = WordsVectorMap(Word2).tail
      val distance = EuclideanDistance.getEuclideanDistance(Vector1, Vector2)
      distance
    }
    else
      -2d
  }
  private def isWordVectorExists(Word:String): Boolean =
  {

    if(WordsVectorMap.contains(Word) && WordsVectorMap(Word).nonEmpty && WordsVectorMap(Word).tail.nonEmpty)
      true
    else
      false
  }
}

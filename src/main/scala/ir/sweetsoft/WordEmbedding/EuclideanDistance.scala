package ir.sweetsoft.WordEmbedding


object EuclideanDistance {

  def getEuclideanDistance(x: Array[Double], y: Array[Double]): Double = {
    require(x.size == y.size)
    var Sigma=0d
//    var i=0
    for(i<-x.indices)
      {
        Sigma=Sigma+math.pow(x(i)-y(i),2)
      }
    math.sqrt(Sigma)
  }

}
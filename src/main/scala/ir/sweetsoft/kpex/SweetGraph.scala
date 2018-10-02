package ir.sweetsoft.kpex

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, graphx}
object SweetGraph {
  def dijkstra[VD](g: Graph[VD, Double], origin: VertexId):Double = {
    var g2 = g.mapVertices(
      (vid, vd) => (false, if (vid == origin) 0 else Double.MaxValue, List[VertexId]())
    )

    for (i <- 1L to g.vertices.count - 1) {
      val currentVertexId: VertexId = g2.vertices.filter(!_._2._1)
        .fold((0L, (false, Double.MaxValue, List[VertexId]())))(
          (a, b) => if (a._2._2 < b._2._2) a else b)._1

      val newDistances: VertexRDD[(Double, List[VertexId])] =
        g2.aggregateMessages[(Double, List[VertexId])](
          ctx => if (ctx.srcId == currentVertexId) {
            ctx.sendToDst((ctx.srcAttr._2 + ctx.attr, ctx.srcAttr._3 :+ ctx.srcId))
          },
          (a, b) => if (a._1 < b._1) a else b
        )

      g2 = g2.outerJoinVertices(newDistances)((vid, vd, newSum) => {
        val newSumVal = newSum.getOrElse((Double.MaxValue, List[VertexId]()))
        (
          vd._1 || vid == currentVertexId,
          math.min(vd._2, newSumVal._1),
          if (vd._2 < newSumVal._1) vd._3 else newSumVal._2
        )
      })
    }

    val distances=g.outerJoinVertices(g2.vertices)((vid, vd, dist) =>
      (vd, dist.getOrElse((false, Double.MaxValue, List[VertexId]()))
        .productIterator.toList.tail
      ))
    val out=distances.vertices.map(_._2._2.head.toString.toDouble)

    val sums=out.reduce((a,b)=>
      if(a+b!=Double.MaxValue)
        a+b
      else if(a!=Double.MaxValue) a
      else if(b!=Double.MaxValue) b
      else 0)
    val out3=if(sums==0) 0 else 1/sums
    out3
  }
  def getDistance(graph: Graph[Int, Double], sourceId: VertexId,DefaultValueForInfinityLength: Double): Double =
  {
    var sum=0d
  val initialGraph = graph.mapVertices((id, _) =>
    if (id == sourceId) 0.0 else Double.PositiveInfinity)
    val sssp = initialGraph.pregel(Double.PositiveInfinity)(
      (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
      triplet => {  // Send Message
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b) // Merge Message

    )
    val vertices=sssp.vertices.collect
    vertices.map{
      v=>
        {
          val value=v._2
          if(value!=Double.PositiveInfinity)
            sum+=value
          else
            sum+=DefaultValueForInfinityLength
          value
        }

    }
    sum
  }
  def getCloseness(sparkContext: SparkContext,graph: Graph[Int, Double], vertexSeq: Seq[Long]): Seq[(Long, Double)] = {
val DefaultValueForInfinityLength=graph.vertices.count*100
    print("\nCalculating Closenesses...")
    var i=0
    val map=vertexSeq.map { v =>
      i+=1
          val ShortestPaths=getDistance(graph, v,DefaultValueForInfinityLength)
          var Weight = 0d
        if(ShortestPaths!=0)
          Weight = 1 / ShortestPaths

          if(i>1) print("\b\b\b")
          if(i>10) print("\b")
          if(i>100) print("\b")
          if(i>1000) print("\b")
          if(i>10000) print("\b")
          (v, Weight)

    }
    print("\n")
    map
  }

  def MyShortestPath(graph: Graph[Int,Double],vertexSeq:Seq[Long]): Unit =
  {
    val sourceId: VertexId = vertexSeq(0) // The ultimate source
  // Initialize the graph such that all vertices except the root have distance infinity.
  val initialGraph = graph.mapVertices((id, _) =>
    if (id == sourceId) 0.0 else Double.PositiveInfinity)
    val sssp = initialGraph.pregel(Double.PositiveInfinity)(
      (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
      triplet => {  // Send Message
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b) // Merge Message
    )
    println(sssp.vertices.collect.mkString("\n"))
  }
  def getDegree(graph: Graph[Int, Double], vertexSeq: Seq[Long]): RDD[(Long, Double)] = {
    val normalizationFactor: Double = 1f / (graph.vertices.count() - 1)
    val degrees: VertexRDD[Int] = graph.degrees.persist()
    val normalized = degrees.map((s => (s._1, s._2 * normalizationFactor)))
    normalized
  }

  def getEccentericity(graph: Graph[Int, Double], vertexSeq: Seq[Long]): RDD[(Long, Double)] = {
    val spgraph = lib.ShortestPaths.run(graph, vertexSeq)
    val Eccentricity = spgraph.vertices.map(vertex => {
      if(vertex._2.values.max!=0) (vertex._1,1d / vertex._2.values.max) else (vertex._1,0.toDouble)
    })
    Eccentricity
  }

  def getNE_Rank(graph: Graph[Int, Double], vertexSeq: Seq[Long]): RDD[(Long, Double)] = {
    val d = 0.85f
    var edgeWeightSum = 0.0
    var i: Long = 0
    var j = i.toLong
    val EdgesRDD = graph.edges.collect()
    EdgesRDD.foreach { Edge =>
      edgeWeightSum += Edge.attr.toLong
    }
    var result: Map[VertexId, Double] = Map()
//    val ShortestPathGraph = lib.WeightedShortestPaths.run(graph, vertexSeq)
//    println("PageRankCalcStarted")
    val VerticesRDD = graph.vertices.sortByKey(true).collect()
    VerticesRDD.foreach { Vi =>
      var sigmaMultiplyToRVj = 0.0
      EdgesRDD.foreach { wji =>
        if (wji.dstId == Vi._1 && wji.srcId < wji.dstId) {
          var InnerSigma = 0.0d
//          println("IFFF")
          EdgesRDD.foreach { wjk =>
            if (wjk.srcId == wji.dstId)
              {
                InnerSigma = InnerSigma+wjk.attr
              }

          }
          if(InnerSigma!=0.0d && result(wji.srcId)!=0.0d)
            sigmaMultiplyToRVj += wji.attr.toLong / InnerSigma * result(wji.srcId)
//          println("Res:"+result(wji.srcId))
        }
      }
      val res = (1 - d) + d  * sigmaMultiplyToRVj
//      println("PageRank:"+res)
      result += (Vi._1 -> res)
    }
    graph.vertices.map(Vertice => (Vertice._1, result(Vertice._1)))
  }

  def getEccentericityAndCloseness(graph: Graph[Int, Double], vertexSeq: Seq[Long]): RDD[(graphx.VertexId, Double, Double)] = {
    val spgraph = lib.WeightedShortestPaths.run(graph, vertexSeq)
    val Eccentricity = spgraph.vertices.map(vertex => {
      (vertex._1, 1d / vertex._2.values.max, vertex._2.values.sum.toDouble)
    })
    Eccentricity
  }
}


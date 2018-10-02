import scala.io.Source
import scala.collection.mutable.HashMap
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/**
  * Created by Ilias Sarantopoulos on 4/27/16.
  * Degree centrality is calculated for each vertex as follows:
  * d(u) = u.degree / Total_number_of_vertices_in_graph -1
  */
object DegreeCentrality {

  def main(edgesRDD: RDD[(VertexId, VertexId)]) {
    val graph = Graph.fromEdgeTuples(edgesRDD, 1)
    val vertexSeq = graph.vertices.map(v => v._1).collect().toSeq
    val normalizationFactor:Float = 1f/(graph.vertices.count()-1)
    val degrees: VertexRDD[Int] = graph.degrees.persist()
    val normalized = degrees.map((s => (s._1, s._2*normalizationFactor)))
    return normalized

  }


}

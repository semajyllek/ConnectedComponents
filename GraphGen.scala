import org.apache.spark.graphx.PartitionStrategy.RandomVertexCut
import org.apache.spark.graphx.{Graph, GraphLoader, util}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark._
import scala.util.Random


object GraphGen {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GraphGenerator")
    val sc = new SparkContext(conf)
    val num_nodes = 5000000
    val num_edges = 10000000
    //val g = util.GraphGenerators.rmatGraph(sc, num_nodes, num_edges)
    //g.edges.map(e => e.srcId + " " + e.dstId).saveAsTextFile("gs://graph_bucket2/rmat5m_10m.txt")
    val new_g = GraphLoader.edgeListFile(sc,"gs://graph_bucket2/facebook_combined.txt")
    val cc = new_g.connectedComponents()
    cc.vertices.map(_._2).distinct.saveAsTextFile("gs://graph_bucket2/final_components.txt")
    val comp_ct = cc.vertices.map(_._2).distinct.count()
    println(s"Number of connected components: $comp_ct")
  }

}

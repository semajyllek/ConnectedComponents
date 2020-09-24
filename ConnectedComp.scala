// James Kelly code for implementing Google's Connected Component finder (and graph transformer)
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object graphfuncs {

  def LargeStar(node_triple: Tuple2[Long, Tuple2[Set[Long], Long]]): List[Tuple2[Long, Tuple2[Set[Long], Long]]] = {
    val (u, (neighbors, temp_min_v)) = node_triple
    val min_v = if (temp_min_v == -1) (neighbors ++ Set(u)).min else temp_min_v
    var new_edges: List[Tuple2[Long, Tuple2[Set[Long], Long]]] = List()
    for (v <- neighbors) {
      if (v >= u) {
        new_edges = (v, (Set(min_v), min_v)) :: new_edges
      }
    }
    new_edges
  }


  def SmallStar(node_triple: Tuple2[Long, Tuple2[Set[Long], Long]]): List[Tuple2[Long, Tuple2[Set[Long], Long]]] = {
    val (u, (neighbors, temp_min_v)) = node_triple
    val min_v = math.min(u, temp_min_v)
    var new_edges: List[(Long, (Set[Long], Long))] = List()
    for (v <- neighbors ++ Set(u)) {
      if (v <= u) {
        new_edges = (v, (Set(min_v), min_v)) :: (min_v, (Set(v), min_v)) :: new_edges
      }
    }
    new_edges
  }

}




object ConnectedComp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Connected Components")
    val sc = new SparkContext(conf)

    val tfile = sc.textFile(args(0))
    var edge_data = tfile.map(line => line.split("\t").map(_.toLong))
      .flatMap(a => List((a(0), Set(a(1))), (a(1), Set(a(0))))).reduceByKey(_ ++ _)
      .map(a => (a._1, (a._2, -1L)))

    var i = 0
    val MAX_ITERS = 10
    var neighbor_lens = edge_data.map(x => (x._1, x._2._1.size))
    var diff_lens = 1L

    while ((i < MAX_ITERS) && (diff_lens > 0)){

      edge_data = edge_data.flatMap(graphfuncs.LargeStar)
        .reduceByKey((a, b) => (a._1 ++ b._1, math.min(a._2, b._2)))

      edge_data = edge_data.flatMap(graphfuncs.SmallStar)
        .reduceByKey((a, b) => (a._1 ++ b._1, math.min(a._2, b._2)))

      val new_neighbor_lens = edge_data.map(x => (x._1, x._2._1.size))
      val joined_lens = neighbor_lens.fullOuterJoin(new_neighbor_lens)
      diff_lens = joined_lens.filter(x => x._2._1 != x._2._2).count()
      neighbor_lens = new_neighbor_lens
      i += 1
    }

    val components = edge_data.filter(x => x._1 == x._2._2).map(x => x._1)
    val comp_ct = components.count()
    println(s"Number of components: $comp_ct")
    println(s"Iterations: $i")
    //components.saveAsTextFile("gs://graph_bucket1/rmat500_50_output.txt")
  }
}








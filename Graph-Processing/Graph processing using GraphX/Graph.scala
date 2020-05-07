import org.apache.spark.graphx.{Graph => Graph1, VertexId,Edge}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object GraphComponents {
  def main ( args: Array[String] ) {
val conf = new SparkConf().setAppName("Graph")
    val sc = new SparkContext(conf)
//Read the input graph and construct the RDD of edges
    val graphConst: RDD[Edge[Long]] = sc.textFile(args(0))
					.map(line => {
 							val (n,a) = line.split(",").splitAt(1);
							(n(0).toLong, a.toList.map(_.toLong))
						})
					.flatMap(c => c._2.map(e => (c._1,e)))
					.map(ng => Edge(ng._1,ng._2,ng._1))


  //Using the graph builder Graph.fromEdges to construct a Graph from the RDD of edges 
Access the VertexRDD and change the value of each vertex to be the vertex ID (initial group number)
  val graph: Graph1[Long, Long] = Graph1
					.fromEdges(graphConst, "d")
					.mapVertices((id, _) => id)




//Call the Graph.pregel method in the GraphX Pregel API to find the connected components. For each vertex, this method changes its group number to the minimum group number of its neighbors (if it is less than its current group number)
    val grp = graph.pregel(Long.MaxValue, 5)(
     (id, dist, newDist) => math.min(dist, newDist),
      triplet => {
        if(triplet.attr < triplet.dstAttr){
          Iterator((triplet.dstId, triplet.attr))
        } 
        else if(triplet.srcAttr < triplet.attr)
	{
          Iterator((triplet.dstId, triplet.srcAttr))
        }
        else
	{
          Iterator.empty
        }
      },
      (a, b) => math.min(a,b)
    )

//Group the graph vertices by their group number and print the group sizes.
     grp.vertices.map(graph => (graph._2, 1))
                                  .reduceByKey(_+_)
				  .sortByKey()
                                  .map(c =>c._1 + "   " + c._2)
		                  .collect()
				  .foreach(println)   



  }
}

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Graph {
  def main(args: Array[ String ]) {
    val conf=new SparkConf().setAppName("Graph")
    val sc = new SparkContext(conf)

/* read the graph from args(0); the group of a graph node is set to the node ID */

    var graph = sc.textFile(args(0))
                      .map(line=> line.split(",")
	              .map(line => line.toLong))
                      .map(n => {(n(0),n(0),n.slice(1,n.length).toList)})




    for(i <- 1 to 5) {
      	graph = graph.flatMap(n => {(n._1, n._2) :: (n._3.map(a=> (a, n._2)))})   /* associate each adjacent neighbor with the node group number + the node itself with its group number*/
					 .reduceByKey((a,b) => (if(a < b) a else b))    /* get the min group of each node */
					 .join(graph.map(c => {(c._1, (c._2, c._3))}))  /* join with the original graph */
					 .map(c => {(c._1,c._2._1,c._2._2._2)})}  /* reconstruct the graph topology */
    


/* finally, print the group sizes */

    graph
	.map(c => (c._2, 1))
	.reduceByKey(_+_)
	.sortByKey()
	.collect().foreach(println)
  }
}

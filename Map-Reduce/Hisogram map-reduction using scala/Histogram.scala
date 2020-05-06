import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection.mutable.ListBuffer

object Histogram {
  def main(args: Array[ String ]) {
	/* ... */
	val conf = new SparkConf().setAppName("Multiply")
	val sc = new SparkContext(conf)
	// Reading dataset from input text file in args(0)
	val m = sc.textFile(args(0))
				.map(line => {
					val a = line.split(",");
					(a(0).toLong,a(1).toLong,a(2).toLong) 

				}
			)
			
	//map tasks to sort the data and reduce tasks to merge and aggregate the data
	val res = m.flatMap( c =>{
	var list = new ListBuffer[(Long,Long)]
	list+=((10000 + c._1 ,1.toLong))
	list+=((20000 + c._2 ,1.toLong))
	list+=((30000 + c._3 ,1.toLong))
	list
	  }).reduceByKey(_ + _).map(c=>((c._1/10000).toInt,c._1%1000,c._2)).collect()
	//prnhere
        res.foreach(println)
	
	
	
				

  }
}


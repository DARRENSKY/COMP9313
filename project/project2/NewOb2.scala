package comp9313.lab6

import org.apache.spark.graphx._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object WordCount{
  def main(args: Array[String]):Unit = {
    val conf = new SparkConf().setAppName("Problem2").setMaster("local")
    val sc = new SparkContext(conf)

    val inputFile = args(0)
    val length = args(1).toInt
    val edge = sc.textFile(inputFile)

    val list_edge = edge.map(a => a.split(" ")).map(a=> Edge(a(1).toLong, a(2).toLong, 2.0))
    val firstgraph = Graph.fromEdges[Double, Double](list_edge, 0.0)

    val secondgraph = firstgraph.mapVertices((id, _) => Set[List[VertexId]](List()))
    //test
    def test(args:Array[String])={
      val myList = Array(1.9, 2.9, 3.4, 3.5)
      for ( x <- myList ) {
        //println(x)
      }
      val total = 0.0;
      for ( i <- 0 to (myList.length - 1)) {
      }
      //println("Total is " + total);
    }
    val thirdgraph = secondgraph.pregel(Set[List[VertexId]](List()), length)(
      (myId, myAttr, receiveTh) => (for{ myselfx <-myAttr if myselfx.size <0 }yield myselfx)
        ++ (for{myselfy <- receiveTh
                if (!myselfy.contains(myId) && myselfy.size < length) || (myselfy.contains(myId) && myselfy(0) == myId && myselfy.size == length)
      }yield myselfy),
      // Vertex Program
      triplet => {
        Iterator((triplet.dstId, (for{ myselfx <- triplet.srcAttr }
          yield myselfx :+ triplet.srcId)))},

      (myselffirst, myselfsecond) => (myselffirst ++ myselfsecond) // Merge Message
    )

    //test
    def test2(args:Array[String])={
      val myList = Array(1.9, 2.9, 3.4, 3.5)
      for ( x <- myList ) {
        //println(x)
      }
      val total = 0.0;
      for ( i <- 0 to (myList.length - 1)) {
      }
      //println("Total is " + total);
    }
    //test start
    val wenxue1 = thirdgraph.vertices.collect()
    val myList1 = Array(1.2, 2.3, 3.4, 3.5)
    val wenxue2 = wenxue1.map(i => i._2)
    val myList2 = Array(2.9, 8.9, 9.4, 1.5)
    val wenxue3 = wenxue2.map(myselfx => for (mye<-myselfx) yield(mye.toSet))
    val myList3 = Array(3.9, 5.9, 3.2, 2.5)
    val wenxue4 = wenxue3.flatten
    val myList4 = Array(4.9, 7.9, 8.1, 4.5)
    val wenxue5 = wenxue4.toSet
    val myList5 = Array(5.9, 6.9, 8.4, 6.5)
    val wenxue6 = wenxue5.filter{myi => myi.size==length}.size
    // test end
    println(wenxue6)

  }
}






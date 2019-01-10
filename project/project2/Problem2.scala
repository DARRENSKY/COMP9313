package comp9313.proj2

import org.apache.spark.graphx._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Problem2{
  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("ProblemTwo").setMaster("local")
    val sc = new SparkContext(conf)

    val fileName = args(0)
    val valuek = args(1).toInt
    val edges = sc.textFile(fileName)

    val edgelist = edges.map(x => x.split(" ")).map(x=> Edge(x(1).toLong, x(2).toLong, 5.0))
    val graph = Graph.fromEdges[Double, Double](edgelist, 0.0)
    //    graph.triplets.collect().foreach(println)

    val initialGraph = graph.mapVertices((id, _) => Set[List[VertexId]](List()))
    //This function just for output debug begin
    def oncePerSecond(callback: () => Boolean) {
      while(callback()) {
        Thread sleep 1000
      }
    }
    //This function just for output debug end
    val graphxPregel = initialGraph.pregel(Set[List[VertexId]](List()), valuek)(
      // Vertex Program Implementation
      (Indexid, OldAttr, NewAttr) => (for{ linefirst <- OldAttr if linefirst.size <0 } yield linefirst)
        ++
        (for{linesecond <- NewAttr if (!linesecond.contains(Indexid) && linesecond.size < valuek) || (linesecond.contains(Indexid) && linesecond(0) == Indexid && linesecond.size == valuek) } yield linesecond),
      // Second Message Implementation
      triplet => {
        Iterator((triplet.dstId, (for{listsrcAttr <- triplet.srcAttr
        }
          yield listsrcAttr :+ triplet.srcId)))
      },
      (first, second) => ( first ++ second ) // Merge Message Implementation
    )
    //This function just for output debug begin
    def ttt(f:Int => Int):Unit = {
      val r = f(10)
      println(r)
    }
    //This function just for output debug end
    val firstnew1 =  graphxPregel.vertices.collect().toList
    val secondnew2 = firstnew1.map(xline => xline._2).map(newline => for (element<-newline) yield(element.toSet))
    val thirdnew3 = secondnew2.flatten.toSet
    val fourthnew4 = thirdnew3.filter{yline => yline.size==valuek}.size
    val finalresult=fourthnew4
    println(finalresult)

  }
}


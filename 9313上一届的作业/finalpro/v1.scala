package comp9313.ass4

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD


object SetSimJoin{

  def main(args: Array[String]) {
    val inputFile = args(0)
    val outputFolder = args(1)
    val threshold = args(2).toDouble
    
    
    val conf = new SparkConf().setAppName("SetSimJoin").setMaster("local")
    //create a scala spark context
    val sc = new SparkContext(conf)
    //load our input data
    val input = sc.textFile(inputFile)
    
    //wordcount 
    //delete 1st element(node) 
    val temp_line =  input.flatMap(x => x.split(" ").drop(1))
    val wordcounts = temp_line.map(word => (word.toInt, 1)).reduceByKey((a, b) => a + b).collectAsMap()
    //val wordcounts= wordcount.sortBy(_._2)
    
    
   // wordcounts.foreach(println)
    //Split by lines and blank
    val line = input.map(x => x.split(" "))
    //.map(x=>(x(0),x.takeRight(x.length-1)))
    var rid_text = line.flatMap(x => {
                val rid = x(0).toInt
                val text = x.takeRight(x.length-1)
                            .map(x=>x.toInt)
                            .sortWith((a,b)=>a.compare(b)<0)
                            .sortWith((a,b)=>wordcounts.get(a).getOrElse(0).compare(wordcounts.get(b).getOrElse(0))<0).toList
                            
                List((x(0).toInt,text))
               })
    //nodes.foreach(println)
    

    
    def inv_list(id:Int,doc:List[Int]):List[(Int,(Int,List[Int]))]={
      var length=doc.length

      doc.take(length-(length*threshold).ceil.toInt +1)       
         .map(x=>(x,(id,doc)))

           
    }
    
      val inv=rid_text.flatMap(x=>inv_list(x._1,x._2)).groupByKey()
      //inverted.foreach(println)
    
      def RID_pair(element:List[(Int,List[Int])]):List[((Int,List[Int]),(Int,List[Int]))]={
        val pairs= element.combinations(2)
        .filter(x=> if(x(1)._2.length>= x(0)._2.length*threshold) true else false)
        .map(x=>( (x(0)._1,x(0)._2), (x(1)._1,x(1)._2)) ).toList
        
         //pairs.foreach(println)
        pairs
      }



      val result=inv.flatMap(x=>RID_pair(x._2.toList)) 
                    .map(x=>((x._1._1,x._2._1), x._1._2.intersect(x._2._2).size.toDouble/x._1._2.union(x._2._2).distinct.size.toDouble))

      
      // result.foreach(println)        

      result.filter(x=>x._2>=threshold)
            .distinct()
            .sortByKey()
            .map{x=>if(x._1._1<x._1._2)x._1+"\t"+x._2 else x._1.swap+"\t"+x._2}
            .saveAsTextFile(outputFolder)
  

  }
}

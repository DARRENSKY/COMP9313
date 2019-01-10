package comp9313.ass4

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object SetSimJoin {
   class SecondarySort(val first:String,val second:String)extends Ordered[SecondarySort]with Serializable{
    override def compare(that:SecondarySort):Int={
      if(that.first.equals(this.first)){
        return (this.second.toInt - that.second.toInt)
      }else{
        return (this.first.toInt - that.first.toInt)
      }
    }
  }
      
  def main(args:Array[String]){
   val inputFile = args(0)
   val output = args(1)
   val t = args(2).toDouble
   val conf = new SparkConf().setAppName("SetSimJoin").setMaster("local")
   
   val sc = new SparkContext(conf)
   
   val input = sc.textFile(inputFile)
   .map(line => line.split(" "))



   .flatMap(x => {
     val min_inter = Math.min(x.length,(x.length - 1 ) - Math.ceil((x.length-1)*t).toInt +2) //cal the prefix character number needed to be output
     for (i <- 1 until min_inter)  yield (x(i),(x(0),x.drop(1))) //output (prefix_character,(str_id,str))
   })
   .groupByKey()//aggregate str by same prefix character
   .filter(_._2.size >= 1)//get rid of these prefix character which have only one element





   .flatMap(x => {
       val x_list = x._2.toList
       for (i <- 0 until x_list.length-1; j <- i+1 until x_list.length) yield


((x_list(i)._1,x_list(j)._1),(x_list(i)._2.length+x_list(j)._2.length-(x_list(i)._2 union x_list(j)._2).toSet.size).toDouble/(x_list(i)._2 union x_list(j)._2).toSet.size.toDouble)


   })//cal sim and output by ((str1_id,str2_id),sim)
   .filter(_._2 >= t).distinct()//get rid of sim<t and keep the distinct one
   .map(x => (new SecondarySort(x._1._1,x._1._2),x._2)).sortByKey().map(x =>"("+x._1.first+","+x._1.second+")"+"\t"+x._2.toString)// sort and output as string
   
   input.saveAsTextFile(output)
    
   sc.stop()
  }
    
   
 
}

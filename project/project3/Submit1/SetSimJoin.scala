package comp9313.project3

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.util.control._
import scala.math.BigDecimal
import scala.math.BigDecimal.RoundingMode

object SetSimJoin{
def main(args: Array[String]) {
val inputFile1 = args(0)
val inputFile2 = args(1)
val outputFolder = args(2)
val threshold = args(3).toDouble
val conf = new SparkConf().setAppName("SetSimJoin")//.setMaster("local")
//create a scala spark context
val sc = new SparkContext(conf)
//load our input data
val input1 = sc.textFile(inputFile1)
val input2 = sc.textFile(inputFile2)



//wordcount

val temp_line1 = input1.flatMap(x => x.split(" ").drop(1))
val temp_line2 = input2.flatMap(x => x.split(" ").drop(1))
val temp = temp_line1 ++ temp_line2



//temp_line.foreach(println)

val wordcounts = temp.map(word => (word.toInt, 1)).reduceByKey((a, b) => a + b).collectAsMap()
//val wordcounts2 = temp_line2.map(word => (word.toInt, 1)).reduceByKey((a, b) => a + b).collectAsMap()


//wordcounts.foreach(println)

//Split by lines and blank
val line1 = input1.map(x => x.split(" "))
val line2 = input2.map(x => x.split(" "))


var rid_text1 = line1.map(x => {

val rid = x(0).toInt
val text = x.takeRight(x.length-1)
.map(x=>x.toInt)
.sortWith((a,b)=>a.compare(b)<0)
.sortWith((a,b)=>wordcounts.get(a).getOrElse(0).compare(wordcounts.get(b).getOrElse(0))<0)
(rid,text)
})
//rid_text1.foreach(println)



var rid_text2 = line2.map(x => {
//val fid = 2.toInt
val rid = x(0).toInt
val text = x.takeRight(x.length-1)
.map(x=>x.toInt)
.sortWith((a,b)=>a.compare(b)<0)
.sortWith((a,b)=>wordcounts.get(a).getOrElse(0).compare(wordcounts.get(b).getOrElse(0))<0)
(rid,text)
})


//  ？？？ var rid_text = rid_text1.union(rid_text2)



val inv1=rid_text1.flatMap(x=> {
var doc = x._2
var rid = x._1
var length=doc.length
var prefix = length-(length*threshold).ceil.toInt +1
var temp = x._2.take(prefix).map(x=>(x,(rid,doc)))
temp

}
).groupByKey()


//inv1.foreach(println)
val inv2=rid_text2.flatMap(x=> {
var doc = x._2
var rid = x._1
var length=doc.length
var prefix = length-(length*threshold).ceil.toInt +1
var temp = x._2.take(prefix).map(x=>(x,(rid,doc)))
temp
}).groupByKey()

val merged = inv1 ++ inv2

val grouped = merged.groupByKey
grouped.foreach(println)
val result=grouped.filter(_._2.size > 1).flatMap(x=>{
val temp = x._2.toList
val input1 = temp(0).toList
val input2 = temp(1).toList
//println(input1)
//println(input2)
for{a <- input1; b <- input2;
if(List(a._2.length,b._2.length).max*threshold<= List(a._2.length,b._2.length).min)
}yield((a._1, b._1),
BigDecimal((a._2.intersect(b._2).size.toDouble)/(a._2.union(b._2).distinct.size.toDouble))
.setScale(6, RoundingMode.HALF_UP).toDouble)

})
//result.foreach(println)
val finalresults = result.filter(x=>x._2>=threshold).distinct().sortByKey().map(x=>x._1+"\t"+x._2)
val finalresults2=finalresults.collect().toList
val savefileoutput = sc.parallelize(finalresults2.toSeq,1).saveAsTextFile(outputFolder)
//result.foreach(println)





}}


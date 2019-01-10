/**
  * Illustrates flatMap + countByValue for wordcount.
  */
package com.oreilly.learningsparkexamples.mini.scala

//package comp9313.project3

//package comp9313.project3

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
    val conf = new SparkConf().setAppName("SetSimJoin").setMaster("local")
    //create a scala spark context
    val sc = new SparkContext(conf)
    //load our input data
    val input1 = sc.textFile(inputFile1)
    val input2 = sc.textFile(inputFile2)



    //wordcount
    //delete 1st element(node)
    val temp_line1 = input1.flatMap(x => x.split(" ").drop(1))

    val temp_line2 = input2.flatMap(x => x.split(" ").drop(1))
    val temp_line = temp_line1.union(temp_line2)
    //temp_line.foreach(println)
    val wordcounts = temp_line.map(word => (word.toInt, 1)).reduceByKey((a, b) => a + b).collectAsMap()

    //val wordcounts= wordcount.sortBy(_._2)
    //wordcounts.foreach(println)

    //Split by lines and blank
    val line1 = input1.map(x => x.split(" "))
    val line2 = input2.map(x => x.split(" "))

    //.map(x=>(x(0),x.takeRight(x.length-1)))
    var rid_text1 = line1.flatMap(x => {
      val fid = 1.toInt
      val rid = x(0).toInt
      val text = x.takeRight(x.length-1)
        .map(x=>x.toInt)
        .sortWith((a,b)=>a.compare(b)<0)
        .sortWith((a,b)=>wordcounts.get(a).getOrElse(0).compare(wordcounts.get(b).getOrElse(0))<0).toList

      List((fid,(rid,text)))
    })
    //rid_text.foreach(println)
    var rid_text2 = line2.flatMap(x => {
      val fid = 2.toInt
      val rid = x(0).toInt
      val text = x.takeRight(x.length-1)
        .map(x=>x.toInt)
        .sortWith((a,b)=>a.compare(b)<0)
        .sortWith((a,b)=>wordcounts.get(a).getOrElse(0).compare(wordcounts.get(b).getOrElse(0))<0).toList

      List((fid,(rid,text)))
    })
    var rid_text = rid_text1.union(rid_text2)


    def inv_list(fid:Int,rid:Int,doc:List[Int]):
    List[(Int,(Int,(Int,List[Int])))]={
      var length=doc.length
      doc.take(length-(length*threshold).ceil.toInt +1)
        .map(x=>(x,(fid,(rid,doc))))
    }

    val inv=rid_text.flatMap(x=>inv_list(x._1,x._2._1,x._2._2)).groupByKey()
    //inv.foreach(println)

    def getindex(element:List[(Int,(Int,List[Int]))]):
    Int={
      var index = 0
      val loop = new Breaks
      loop.breakable{
        for(i <- element){
          if (i._1 == 2){
            index = element.indexOf(i)
            loop.break
          }
        }
      }
      return index.toInt
    }
    def getPairs(element:List[(Int,(Int,List[Int]))]):
    List[((Int,(Int,List[Int])),(Int,(Int,List[Int])))]={
      //(Int,(Int,List[Int])) = {

      val index = getindex(element)
      var result = List(((0,(0,List(0))),(0,(0,List(0)))))
      for (a <- 0 to index-1){
        for (b <- index to element.length-1){
          //result = List(element(a),element(b))
          result = result ++ List(((element(a)._1, (element(a)._2._1,element(a)._2._2.toList)), (element(b)._1,
            (element(b)._2._1,element(b)._2._2.toList))))

        }
      }
      result = result.drop(1)

      return result

    }

    def RID_pair(element:List[(Int,(Int,List[Int]))]):
    List[((Int,List[Int]),(Int,List[Int]))]={
      val pairs= getPairs(element)
        .filter(x=> if(x._2._2._2.length>= x._1._2._2.length*threshold) true else false)
        .map(x=>( (x._1._2._1,x._1._2._2), (x._2._2._1,x._2._2._2))).toList

      //pairs.foreach(println)
      pairs
    }
    val result=inv.flatMap(x=> RID_pair(x._2.toList))
      .map(x=>((x._1._1,x._2._1), BigDecimal((x._1._2.intersect(x._2._2).size.toDouble)/(x._1._2.union(x._2._2).distinct.size.toDouble)).setScale(6, RoundingMode.HALF_UP).toDouble))

//    result.foreach(println)

//    result.filter(x=>x._2>=threshold)
//      .distinct()
//      .sortByKey()
//      .map(x=>x._1+"\t"+x._2)
//      .saveAsTextFile(outputFolder)
//    result.foreach(println)
    val finalresults = result.filter(x=>x._2>=threshold).distinct().sortByKey().map(x=>x._1+"\t"+x._2)
    val finalresults2=finalresults.collect().toList
    val savefileoutput = sc.parallelize(finalresults2.toSeq,1).saveAsTextFile(outputFolder)
  }
}



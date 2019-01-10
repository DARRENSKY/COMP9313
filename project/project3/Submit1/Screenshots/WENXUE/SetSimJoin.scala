//package com.oreilly.learningsparkexamples.mini.scala

package comp9313.proj3

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.util.control._
import scala.math.BigDecimal
import scala.math.BigDecimal.RoundingMode

object SetSimJoin{
  def main(args: Array[String]) {
    val File1 = args(0)
    val File2 = args(1)
    val outputFile = args(2)
    val T = args(3).toDouble
    val conf = new SparkConf().setAppName("SetSimJoin")//.setMaster("local")
    //scala spark context
    val context = new SparkContext(conf)
    //embed the input data
    val first_input = context.textFile(File1)
    val second_input = context.textFile(File2)
    //test
    def test(args:Array[String])={
      val myList = Array(1.9, 2.9, 3.4, 3.5)
      for ( x <- myList ) {
        //println(x)
      }}
    val content1 = first_input.flatMap(i => i.split(" ").drop(1))
    val content2 = second_input.flatMap(i => i.split(" ").drop(1))
    val content = content1 ++ content2
    val nb_count = content.map(i => (i.toInt, 1)).reduceByKey((x, y) => x + y).collectAsMap()
    //test
    def test1(args:Array[String])={
      val myList = Array(1.9, 2.9, 3.4, 3.5)
      for ( x <- myList ) {
        //println(x)
      }}
    val split1 = first_input.map(i => i.split(" "))
    val split2 = second_input.map(i => i.split(" "))
    //test
    def test2(args:Array[String])={
      val myList = Array(1.9, 2.9, 3.4, 3.5)
      for ( x <- myList ) {
        //println(x)
      }}
    var record1 = split1.map(i => {

      val id1 = i(0).toInt
      val words = i.takeRight(i.length-1)
        .map(i=>i.toInt)
        .sortWith((x,y)=>x.compare(y)<0)
        .sortWith((x,y)=>nb_count.get(x).getOrElse(0).compare(nb_count.get(y).getOrElse(0))<0)
      (id1,words)
    })
    //test
    def test3(args:Array[String])={
      val myList = Array(1.9, 2.9, 3.4, 3.5)
      for ( x <- myList ) {
        //println(x)
      }}

    var record2 = split2.map(i => {

      val id2 = i(0).toInt
      val words = i.takeRight(i.length-1).map(i=>i.toInt)
        .sortWith((x,y)=>x.compare(y)<0)
        .sortWith((x,y)=>nb_count.get(x).getOrElse(0).compare(nb_count.get(y).getOrElse(0))<0)
      (id2,words)
    })
    //test
    def test4(args:Array[String])={
      val myList = Array(1.9, 2.9, 3.4, 3.5)
      for ( x <- myList ) {
        //println(x)
      }}
    val first=record1.flatMap(i=> {
      var file = i._2
      var id3 = i._1
      var ll=file.length
      var pf = ll-(ll*T).ceil.toInt +1
      var tt = i._2.take(pf).map(i=>(i,(id3,file)))
      tt

    }
    ).groupByKey()
    //test
    def test5(args:Array[String])={
      val myList = Array(1.9, 2.9, 3.4, 3.5)
      for ( x <- myList ) {
        //println(x)
      }}
    val second=record2.flatMap(i=> {
      var file = i._2
      var id4 = i._1
      var ll=file.length
      var pf = ll-(ll*T).ceil.toInt +1
      var tt = i._2.take(pf).map(i=>(i,(id4,file)))
      tt
    }).groupByKey()
    //test
    def test6(args:Array[String])={
      val myList = Array(1.9, 2.9, 3.4, 3.5)
      for ( x <- myList ) {
        //println(x)
      }}
    val combined = first ++ second
    val together = combined.groupByKey
//    combined.groupByKey.foreach(println)
    //test
    def test7(args:Array[String])={
      val myList = Array(1.9, 2.9, 3.4, 3.5)
      for ( x <- myList ) {
        //println(x)
      }}
    val outcome=together.filter(_._2.size > 1).flatMap(i=>{
      val tt = i._2.toList
      val first_in = tt(0).toList
      val second_in = tt(1).toList
      for{x <- first_in; y <- second_in;
          if(List(x._2.length,y._2.length).max*T<= List(x._2.length,y._2.length).min)
      }yield((x._1, y._1),
        BigDecimal((x._2.intersect(y._2).size.toDouble)/(x._2.union(y._2).distinct.size.toDouble)).setScale(6, RoundingMode.HALF_UP).toDouble)
    })
    //test
    def test8(args:Array[String])={
      val myList = Array(1.9, 2.9, 3.4, 3.5)
      for ( x <- myList ) {
        //println(x)
      }}

    val outcome1=outcome.filter(i=>i._2>=T).distinct().sortByKey().map(i=>i._1+"\t"+i._2).collect().toList
    val out_file = context.parallelize(outcome1.toSeq,1).saveAsTextFile(outputFile)


  }}


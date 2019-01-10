//package com.oreilly.learningsparkexamples.mini.scala

package comp9313.proj3

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
    //argument 0 is first file
    val firstFile = args(0)
    //argument 1 is second file
    val secondFile = args(1)
    //argument 2 is output folder (save file name)
    val outputFolder = args(2)
    //argument 3 is threshold
    val bottom = args(3).toDouble
    // spark configuration if aws remove setMaster
    val conf = new SparkConf().setAppName("SetSimJoin")//.setMaster("local")
    val sc = new SparkContext(conf)
    //load first file context by sc
    val sc_firstFileRDD = sc.textFile(firstFile)
    //load second file context by sc
    val sc_secondFileRDD = sc.textFile(secondFile)

    //file include doc id + doc context
    //step 1: remove doc id by flat map drop(1)
    val doc1_only_text = sc_firstFileRDD.flatMap(id_plus_text => id_plus_text.split(" ").drop(1))
    val doc2_only_text = sc_secondFileRDD.flatMap(id_plus_text => id_plus_text.split(" ").drop(1))
    //step 2: merge file1 and file2 to get all doc context
    val doc1_2_only_text = doc1_only_text ++ doc2_only_text

    //step 3: count all doc id frequency such as wordcount.scala
    val wordTotal = doc1_2_only_text.map(docid => (docid.toInt, 1)).reduceByKey((id_one, id_two) => id_one + id_two).collectAsMap()

    //step 4: get all file context including id and context
    val doc1_id_text = sc_firstFileRDD.map(word => word.split(" "))
    val doc2_id_text = sc_secondFileRDD.map(word => word.split(" "))

    //This function just for output debug begin
    def ttt(f:Int => Int):Unit = {
      val r = f(10)
      println(r)
    }
    //This function just for output debug end

    //step 4: sort file context except id by its number (e.g. 980>600) and then its word count frequency
    val docid_textOne = doc1_id_text.map(line => {(line(0).toInt,line.takeRight(line.length-1).map(docid=>docid.toInt).sortWith((first,second)=>first.compare(second)<0).sortWith((first,second)=>wordTotal.get(first).getOrElse(0).compare(wordTotal.get(second).getOrElse(0))<0))})
    val docid_textTwo = doc2_id_text.map(line => {(line(0).toInt,line.takeRight(line.length-1).map(docid=>docid.toInt).sortWith((first,second)=>first.compare(second)<0).sortWith((first,second)=>wordTotal.get(first).getOrElse(0).compare(wordTotal.get(second).getOrElse(0))<0))})

    //step 5: map all context and group by key by perfix filter method see report
    val inverted_file1 = docid_textOne.flatMap(x1 => {x1._2.take(x1._2.length-(x1._2.length*bottom).ceil.toInt +1).map(x2=>(x2,(x1._1,x1._2)))}).groupByKey()
    val inverted_file2 = docid_textTwo.flatMap(x1 => {x1._2.take(x1._2.length-(x1._2.length*bottom).ceil.toInt +1).map(x2=>(x2,(x1._1,x1._2)))}).groupByKey()

    //step 6: merge two inverted lists
    val mergeAndgroup=(inverted_file1 ++ inverted_file2).groupByKey()
    //test function
    def  calFrequency(terms:List[(String,String)]) = {
      val wordsFirst = terms.groupBy(identity).map(linex=>(linex._1,linex._2.size))
      val wordsScond = terms.groupBy(_._1).map(linex=>(linex._1,linex._2.size))
      wordsFirst.map(linex=>(linex._1,linex._2*1.0/wordsScond.get(linex._1._1).get))}
    //test
    //step 7: Finding “similar” id pairs (it size >1) by sim(r, s) >= τ, l = |r intersect s| >= |r union s| * τ >= max(|r|, |s|) * τ
    val incomplete_result=mergeAndgroup.filter(_._2.size > 1).flatMap(buffer_id_text=>{
      for{file1_doc <- buffer_id_text._2.toList(0).toList; file2_doc <- buffer_id_text._2.toList(1).toList;
          if(List(file1_doc._2.length,file2_doc._2.length).min >= List(file1_doc._2.length,file2_doc._2.length).max*bottom)
        //step 8: get the result by Jaccard Similarity: sim(r, s) = |r intersect s|/|r union s|
      }yield((file1_doc._1, file2_doc._1), BigDecimal((file1_doc._2.intersect(file2_doc._2).size.toDouble)/(file1_doc._2.union(file2_doc._2).distinct.size.toDouble)).setScale(6, RoundingMode.HALF_UP).toDouble)
    })
    //This function just for output debug begin
    def oncePerSecond(callback: () => Boolean) {
      while(callback()) {
        Thread sleep 1000
      }
    }
    //This function just for output debug end
    //step 9: filter result by bigger and equal threshold and remove duplicate results
    val complete_result = incomplete_result.filter(id1id2_answer=>id1id2_answer._2>=bottom).distinct().sortByKey().map(tuple_answer=>tuple_answer._1+"\t"+tuple_answer._2).collect().toList
    // save all result to one file
    val final_save = sc.parallelize(complete_result.toSeq,1).saveAsTextFile(outputFolder)

  }
}


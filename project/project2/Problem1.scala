package comp9313.proj2

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Problem1 {
  def main(args: Array[String]) {
    
    val inputFile = args(0)
    val outputFolder = args(1)
    val conf = new SparkConf().setAppName("Problem1").setMaster("local")
    val sc = new SparkContext(conf)

    val inputcontent = sc.textFile(inputFile)
    val words = inputcontent.map(_.toLowerCase.split("[\\s*$&#/\"'\\,.:;?!\\[\\](){}<>~\\-_]+").map(_.trim));

    val coTermWords = words.flatMap{ Termline => for{ i <-0 until Termline.length ; j <- (i+1) until Termline.length} yield {(Termline(i), Termline(j))}}.filter(_._1.matches("^[a-z].*")).filter(_._2.matches("^[a-z].*"))

    def  calFrequency(terms:List[(String,String)]) = {
      val wordsFirst = terms.groupBy(identity).map(linex=>(linex._1,linex._2.size))
      val wordsScond = terms.groupBy(_._1).map(linex=>(linex._1,linex._2.size))
      wordsFirst.map(linex=>(linex._1,linex._2*1.0/wordsScond.get(linex._1._1).get))}

    val list: List[(String, String)] = coTermWords.collect().toList

    val result = calFrequency(list)

    val newrdd = sc.parallelize(result.toSeq)

    val first=newrdd.sortBy(line => line._1._2).sortBy(line => line._2, false).sortBy(line => line._1._1)

    val finalresults=first.map(line => (line._1._1 +" "+ line._1._2 +" "+ line._2))

    val finalresults2=finalresults.collect().toList

    val savefileoutput = sc.parallelize(finalresults2.toSeq,1).saveAsTextFile(outputFolder)

  }
}
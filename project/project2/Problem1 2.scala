//package comp9313.lab6

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Problem1 {
  def main(args: Array[String]) {
    // initial
    val inputFile = args(0)
    val outputFolder = args(1)
    val conf = new SparkConf().setAppName("Problem1").setMaster("local")
    val sc = new SparkContext(conf)

//    // split the each line of input file into an array of words and sort it
//    val vocabulary = sc.textFile("input").map(_.toLowerCase.split("[\\s*$&#/\"'\\,.:;?!\\[\\](){}<>~\\-_]+").map(_.trim))
//
//    // map all words into word pairs, for example (word1, word2),and filter these words
//    val wordpair = vocabulary.flatMap{ numbLine=>
//      for{
//        a <-0 until numbLine.length
//        b <- (a+1) until numbLine.length
//      } yield {
//        (numbLine(a), numbLine(b))
//      }}.filter(_._1.matches("^[a-z].*")).filter(_._2.matches("^[a-z].*"))
//
//    // compute the frequency of word pairs
//    def  pairFre(words:List[(String,String)]) = {
//      val first = words.groupBy(identity).map(c=>(c._1,c._2.size))
//      val second = words.groupBy(_._1).map(c=>(c._1,c._2.size))
//      first.map(c=>(c._1,c._2*1.0/second.get(c._1._1).get))
//    }
//
//    val tolist: List[(String, String)] = wordpair.collect().toList
//    val outcome = pairFre(tolist)
//    val dataset = sc.parallelize(outcome.toSeq)
//    val first=dataset.sortBy(x => x._1._2).sortBy(x => x._2, false).sortBy(x => x._1._1)
//    val outcome1=first.map(x => (x._1._1 +" "+ x._1._2 +" "+ x._2))
//    val outcome2=outcome1.collect().toList
//    val outfile = sc.parallelize(outcome2.toSeq,1).saveAsTextFile(outputFolder)

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
package comp9313.ass3

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._

object Problem2 {
	def main(args: Array[String]) {
		// Problems 2 correct
    val inputFile = args(0)
    val outputFolder = args(1)
    val k = args(2).toInt
    val conf = new SparkConf().setAppName("Problem1").setMaster("local")
    val sc = new SparkContext(conf)
    
    // split the each line of input file into an array of words and sort it
		val words = sc.textFile(inputFile).map(_.toLowerCase.split("[\\s*$&#/\"'\\,.:;?!\\[\\](){}<>~\\-_]+").map(_.trim).sorted)
		
		// map all words in an array into co-term pairs, like (word1, word2)
		// and filter those words without startwith "a-z" 
		val coTerm = words.flatMap{ line =>
				for{ 
    			i <-0 until line.length
    			j <- (i+1) until line.length
				} yield {
    			(line(i), line(j))
				}}.filter(_._1.matches("^[a-z].*")).filter(_._2.matches("^[a-z].*"))
		
		// map co-term pair with count 1 and reduce that by key(co-term) and take first 10 
		val top10CoTerms = coTerm.map((_, 1)).reduceByKey(_ + _).map(_.swap).sortByKey(false).take(k).map(_.swap)
    val results = top10CoTerms.map(line => (line._1._1 +"," + line._1._2 + "\t" +line._2))
    sc.parallelize(results.toSeq).saveAsTextFile(outputFolder)
  }    
}
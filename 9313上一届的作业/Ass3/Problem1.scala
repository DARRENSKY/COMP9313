package comp9313.ass3

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._

object Problem1 {
  var PostId = 1
  var VoteTypeId = 2
  var UserId = 3

  def Question1(textFile: RDD[Array[String]]) {
		// Problem 1.1
  	// map all distinct (postID, VoteTypeID) into (VoteTypeID, 1) and reduce that by key
		val countPosts = textFile.map(line => (line(PostId), line(VoteTypeId))).distinct().map(line => (line._2, 1)).reduceByKey(_ + _)
		
		// swap key with value and sort that and take the top-five
		val top5Posts = countPosts.map(_.swap).sortByKey(false).take(5).map(_.swap)  
		top5Posts.foreach(line => println(line._1 + "\t" + line._2))
  }
  
  def Question2(textFile: RDD[Array[String]]) {
		// Problem 1.2
  	// find all PostID and UserID which VoteTypeID is 5 and map that to (PostId, (UserID, 1))
  	val favorPosts = textFile.filter(line => line(VoteTypeId) == "5").map(line => (line(PostId), (line(UserId), 1)))
		
  	// count the pair-(PostID, (UserID, 1)) by key into (key, (arryaOfAllUsers, totalCount)
  	val countFP = favorPosts.reduceByKey{case ((x1, y1), (x2, y2)) => (x1 + "," + x2, y1 + y2)}
  	
  	// find the top10
    val top10Favorite = countFP.filter(line => line._2._2 > 10).map{case(k, (v1, v2)) => (k.toInt, v1)}.sortByKey()
  	
    // sort the array of users for each PostID
    val result = top10Favorite.map(line => (line._1, line._2.split(",").map(_.toInt).sorted.mkString(",")))
    result.foreach(line => println(line._1 + "#" + line._2))
  }

  def main(args: Array[String]) {
    val inputFile = args(0)
    val conf = new SparkConf().setAppName("Problem1").setMaster("local")
    val sc = new SparkContext(conf)
    
    val textFile = sc.textFile(inputFile).map(_.split(","))
    println("Question 1 Answer:")
    Question1(textFile)
    println("Question 2 Answer:")
    Question2(textFile)
  }  
}
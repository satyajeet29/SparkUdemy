package pradhan.com.spark
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SaveMode

// $example on:init_session$
import org.apache.spark.sql.SparkSession
// $example off:init_session$

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
object RatingsCounter {
  //Our main function where all the action happens
  def main(args: Array[String]){
    
    //Set the log level to only print error
    Logger.getLogger("org").setLevel(Level.ERROR) 
    
    
    //Create a Spark Context using every core of the local machine named RatingsCounter
    val sc = new SparkContext("local[*]","RatingsCounter")
    
    //Load up each line of the ratings data into an RDD
    val lines = sc.textFile("../ml-100k/u.data")
    
    //Convert each line to a string, split it out by tabs and extract the third field
    // The file format is userID, movieID, rating, timestamp
    val ratings = lines.map(x => x.toString().split("\t")(2))
    
    //Count up how many times each value rating occurs
    val results = ratings.countByValue()
    
    //Sort the resulting map of (rating, count) tuples
    val sortedResults = results.toSeq.sortBy(_._1)
    
    //Print each result in it's own line
    sortedResults.foreach(println)
    
  }
}
package com.mgohain.wordcount

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by mgohain on 3/16/2016.
  */
object WordCount {
  def main(args: Array[String]) = {
    if (args.length != 2) {
      println("In correct number of arguments " + args.length)
      System.exit(0)
    }
    val input = args(0)
    val output = args(1)
    val conf = new SparkConf()
    conf.setAppName("word counter")
    conf.setMaster("local[2]")
    val sparkConf = new SparkContext(conf)
    val lines = sparkConf.textFile(input)
    val words = lines.flatMap(line => line.split(" "))
    val wordCount = words.map(word => (word, 1)).reduceByKey(_ + _)
    wordCount.saveAsTextFile(output)
  }
}

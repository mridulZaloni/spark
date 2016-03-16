package com.mgohain.colfrequency

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * Created by mgohain on 3/16/2016.
  */
class ColFrequency {
  def main(args : Array[String]) = {
    val conf = new SparkConf()
    conf.setAppName("Column-Frequency-Counter")
    conf.setMaster("local[1]")
    val context = new SparkContext(conf)
    var l = new ListBuffer[String]
    val lines = context.textFile(args(0))

    lines.collect().foreach((a:String) => l += a.split(" ")(1))
    val list = l.toList
    val col2 = context.parallelize(list)
    val freq = col2.map(value => (value, 1)).reduceByKey(_+_)
    freq.saveAsTextFile(args(1))
  }
}

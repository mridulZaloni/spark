package com.zaloni.mgohain.learnspark.secondary_sorting

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by mgohain on 3/4/2016.
  */

object SecondarySort {

  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setAppName("SecondarySort").setMaster("local[4]")
    val sparkContext = new SparkContext(conf)
    val flightData = sparkContext.textFile(args(0))
    val flightDataTokens = flightData.map(data => data.split(","))
    val keyValueData = flightDataTokens.map(arr => createKeyValue(arr))
    val sortedData = keyValueData.repartitionAndSortWithinPartitions(new FlightPartitioner(2))
    sortedData.saveAsTextFile(args(1))
  }
  def createKeyValue(data: Array[String]): (FlightKey, List[String]) = {
    (createKey(data), createValue(data))
  }
  def createKey(data: Array[String]): FlightKey = {
    FlightKey(data(0), data(3), data(9), data(18))
  }
  def createValue(data: Array[String]): List[String] = {
    List(data(4), data(6), data(7), data(10), data(11))
  }
}

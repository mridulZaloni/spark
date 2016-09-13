package com.zaloni.mgohain.learnspark.secondary_sorting

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by mgohain on 9/13/2016.
  */

object SecondarySort {

  def main(args: Array[String]) {
    if (args.length != 2) {
      println("In correct number of arguments.")
      System.exit(1)
    }
    val conf = new SparkConf()
    conf.setAppName("SecondarySort").setMaster("local[4]")
    val sparkContext = new SparkContext(conf)
    val flightData = sparkContext.textFile(args(0))
    val flightDataTokens = flightData.map(data => data.split(","))
    val keyValueData = flightDataTokens.map(arr => createKeyValue(arr))
    val sortedData = keyValueData.repartitionAndSortWithinPartitions(new FlightPartitioner(2))
    sortedData.collect().foreach(println)
    sortedData.saveAsTextFile(args(1))
  }

  /**
    * Method used to create the keyValue pair from the original data
    * @param data Record holding flight information
    * @return returns a key value pair
    */
  def createKeyValue(data: Array[String]): (FlightKey, List[String]) = {
    (createKey(data), createValue(data))
  }

  /**
    * Method to create the key part
    * @param data Record holding flight information
    * @return returns an instance of FlightKey
    */
  def createKey(data: Array[String]): FlightKey = {
    FlightKey(data(4), data(5), data(15), data(22))
  }

  /**
    * Method to create the value part
    * @param data Record holding flight information
    * @return Returns a list consisting of required data
    */
  def createValue(data: Array[String]): List[String] = {
    List(data(0), data(1), data(2), data(8), data(13), data(19), data(20), data(24), data(25))
  }
}

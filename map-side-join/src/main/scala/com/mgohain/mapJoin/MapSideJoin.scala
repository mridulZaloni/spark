package com.mgohain.mapJoin

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by mgohain on 3/16/2016.
  */
class MapSideJoin {
  def main(args: Array[String]) {
    if (args.length != 3) {
      System.out.println("Incorrect number of arguments " + args.length)
      System.exit(0)
    }
    val sparkConf = new SparkConf()
    sparkConf.setAppName("MapSideJoin").setMaster("local[2]")
    val sparkContext = new SparkContext(sparkConf)
    val deptData = sparkContext.textFile(args(0))
    val empData = sparkContext.textFile(args(1))
    val deptDataMap = sparkContext.broadcast(deptData.map(dept => (dept.split(",")(0).trim, dept.split(",")(1))).collectAsMap())
    val joinedData = empData.map{emp => (emp.split(",")(0), emp.split(",")(1), deptDataMap.value.get(emp.split(",")(2).trim))}
    joinedData.saveAsTextFile(args(2))
  }
}

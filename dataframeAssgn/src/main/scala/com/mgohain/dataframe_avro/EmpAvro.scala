package com.mgohain.dataframe_avro

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import com.databricks.spark.avro._
/**
  * Created by mgohain on 7/20/2016.
  */
object EmpAvro {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("Employee Avro Example").setMaster("local[2]")
    val context = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(context)
    val records = sqlContext.read.format("com.databricks.spark.avro").load(args(0))
    records.printSchema()
    records.filter("emp_id=1").collect().foreach(println)
  }
}

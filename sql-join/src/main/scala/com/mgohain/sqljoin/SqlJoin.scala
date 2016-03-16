package com.mgohain.sqljoin

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by mgohain on 3/16/2016.
  */
class SqlJoin {
  def main(args: Array[String]) {
    if (args.length != 3) {
      System.out.println("Incorrect number of arguments " + args.length)
      System.exit(0)
    }
    val empFile = args(0)
    val deptFile = args(1)
    val output = args(2)
    val sparkConf = new SparkConf()
    sparkConf.setAppName("Join with SparkSql").setMaster("local[2]")
    val sparkContext = new SparkContext(sparkConf)
    /**
      * Creating RDD of employes
      */
    val employees = sparkContext.textFile(empFile)
    /**
      * Creating RDD of departments
      */
    val departments = sparkContext.textFile(deptFile)
    /**
      * Creating SQLContext
      */
    val sqlContext = new SQLContext(sparkContext)
    /**
      * Defining the schema string
      */
    val empSchemaString = "id,name,deptId"
    val deptSchemaString = "id,name"

    /**
      * Defining the schema based on the schema string
      */
    val empSchema = StructType(empSchemaString.split(",")
      .map(fieldName => StructField(fieldName, StringType, true)))
    val deptSchema = StructType(deptSchemaString.split(",")
      .map(fieldName => StructField(fieldName, StringType, true)))

    /**
      * converting the RDDs into SQlRows
      */
    val empRows = employees.map(emp => emp.split(",")).map(emp => Row(emp(0), emp(1), emp(2)))
    val deptRows = departments.map(_.split(",")).map(dept => Row(dept(0), dept(1)))

    /**
      * Creating the dataframes with corresponding schema
      */
    val empDf = sqlContext.createDataFrame(empRows, empSchema)
    val deptDf = sqlContext.createDataFrame(deptRows, deptSchema)

    /**
      * Registering dataframes as tables
      */

    empDf.registerTempTable("employees")
    deptDf.registerTempTable("departments")

    /**
      * Retrieving the join results
      */
    val joinResult = sqlContext.sql("select E.id, E.name, D.name from employees E JOIN departments D ON E.deptId = D.id")

    /**
      * Saving the joined result
      */
    joinResult.rdd.repartition(1).saveAsTextFile(output)
  }
}

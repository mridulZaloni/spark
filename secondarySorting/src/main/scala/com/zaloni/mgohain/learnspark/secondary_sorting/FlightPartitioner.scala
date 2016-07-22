package com.zaloni.mgohain.learnspark.secondary_sorting

import org.apache.spark.Partitioner

/**
  * Created by mgohain on 3/4/2016.
  */
/**
  * Custom partitioner
  * @param partitions number of required partitions
  */
class FlightPartitioner(partitions : Int) extends Partitioner {
  override def numPartitions : Int = partitions
  override def getPartition(key: Any): Int = {
    val flightKey = key.asInstanceOf[FlightKey]
    flightKey.carrier.hashCode() % numPartitions
  }
}

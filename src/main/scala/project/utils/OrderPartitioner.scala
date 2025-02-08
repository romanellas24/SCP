package project.utils

import org.apache.spark.Partitioner

class OrderPartitioner(override val numPartitions: Int) extends Partitioner {
  def getPartition(key: Any): Int = key match {
    case s: Int =>
      s % numPartitions
    case _ => throw new IllegalArgumentException("Invalid key type")
  }
}

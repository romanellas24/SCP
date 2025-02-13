package project.utils

import org.apache.spark.Partitioner

class NewOrderPartitioner(override val numPartitions: Int) extends Partitioner {

  def getPartition(key: Any): Int = key match {
    case s: Int => {
      (s / 100) % numPartitions
    }
    case _ => throw new IllegalArgumentException("Invalid key type")
  }
}

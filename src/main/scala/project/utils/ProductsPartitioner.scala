package project.utils

import org.apache.spark.Partitioner

class ProductsPartitioner(override val numPartitions: Int) extends Partitioner {

  def getPartition(key: Any): Int = key match {
    case s: (Int, Int) => {
      val (x, _) = s
      (x / 100) % numPartitions
    }
    case _ => {
      throw new IllegalArgumentException("Invalid key type")
    }
  }
}

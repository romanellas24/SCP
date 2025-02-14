package project

import org.apache.spark.sql.SparkSession
import project.Second.{clock, partitioned}
import project.utils.{Clock, OrderPartitioner}


object Fourth extends App {

  private val clock = new Clock()
  println("Hello! Scala is working")
  private val spark = SparkSession
    .builder()
    .appName("SCP") //TODO: RECOMMENT BELLOW
    .master("local[*]")
    .config("spark.executor.memory", "4G")
    .config("spark.driver.memory", "4G")
    .config("spark.driver.maxResultSize", "4G")
    .config("spark.memory.offHeap.enabled", true)
    .config("spark.memory.offHeap.size", "4G")
    .getOrCreate()
  println("Spark is running!")
  //private val rdd = spark.read.csv("./data/order_products.csv").rdd
  private val rdd = spark.read.csv("./data/medium.csv").rdd
  //private val rdd = spark.read.csv("./data/small.csv").rdd
  //private val rdd = spark.read.csv("gs://order-dataset/data/order_products.csv").rdd

  //Given an RDD[String] We'll parse all as (O, P) Where, O is the order and P is the product
  private val orderProductPair = rdd.map(e => {
    val tmp = e.toString().replace("[", "").replace("]", "").split(",")
    (tmp.head.toInt, tmp.tail.head.toInt)
  })

  private val partitions = 100
  private val partitioner = new OrderPartitioner(partitions)
  private val partitioned = orderProductPair.partitionBy(partitioner)
  //val test = partitioned.groupBy(e => e._1)
  val test = partitioned.groupByKey().mapValues(iterable => iterable)
    .flatMapValues(productIds =>
    for {
      x <- productIds
      y <- productIds
      if x < y
    } yield (x, y)
  ).map(iterable => iterable._2)
    .groupBy(e => e).map(e => {
    val (x, y) = e._1
    (x, y, e._2.size)
  })

  //println(test.collect().toList)

  //val df = spark.createDataFrame(test).repartition(1)
  //df.write.format("csv").option("path", "gs://order-dataset/out/out-second-thirty-second.csv").save()
  //df.write.format("csv").option("path", "out.csv").save()
  clock.printElapsedTime()
}

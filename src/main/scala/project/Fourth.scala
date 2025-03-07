package project

import org.apache.spark.sql.SparkSession
import project.utils.{Clock, OrderPartitioner, ProductsPartitioner}


object Fourth extends App {

  private val clock = new Clock()
  println("Hello! Scala is working")
  private val spark = SparkSession
    .builder()
    .appName("SCP")
    //.master("local[*]")
    .getOrCreate()
  println("Spark is running!")
  //private val rdd = spark.read.csv("./data/order_products.csv").rdd
  //private val rdd = spark.read.csv("./data/medium.csv").rdd
  //private val rdd = spark.read.csv("./data/small.csv").rdd
  private val filename = args.apply(0)
  private val rdd = spark.read.csv("gs://order-dataset/data/" + filename).rdd

  //Given an RDD[String] We'll parse all as (O, P) Where, O is the order and P is the product
  private val orderProductPair = rdd.map(e => {
    val tmp = e.toString().replace("[", "").replace("]", "").split(",")
    (tmp.head.toInt, tmp.tail.head.toInt)
  })

  private val partitions = 96
  private val partitioner = new OrderPartitioner(partitions)
  private val partitioned = orderProductPair.partitionBy(partitioner)
  val test = partitioned.groupByKey()
    .flatMapValues(productIds =>
      for {
        x <- productIds
        y <- productIds
        if x < y
      } yield (x, y)
    ).map(iterable => (iterable._2._1, iterable._2._2))
    .groupBy(e => e).map(e => {
      val (x, y) = e._1
      (x, y, e._2.size)
    })

  //println(test.collect().toList)
  val df = spark.createDataFrame(test).repartition(1)
  df.write.format("csv").option("path", "gs://order-dataset/out/out-fourth-" + filename).save()
  //df.write.format("csv").option("path", "out.csv").save()
  clock.printElapsedTime()
}

package project

import org.apache.spark.sql.SparkSession
import project.utils.{Clock, Printer};

object First extends App {
  private val clock = new Clock()
  println("Hello! Scala is working")
  private val spark = SparkSession
    .builder()
    .appName("SCP")
    /*
    .master("local[*]")
    .config("spark.executor.memory", "128g")
    .config("spark.driver.memory", "128g")
    .config("spark.driver.maxResultSize", "16g")
    .config("spark.memory.offHeap.enabled",true)
    .config("spark.memory.offHeap.size", "128g")
     */
    .getOrCreate()

  println("Spark is running!")
  //private val rdd = spark.read.csv("./data/order_products.csv").rdd
  //private val rdd = spark.read.csv("./data/small.csv").rdd
  //private val rdd = spark.read.csv("./data/order_products.csv").rdd
  //private val rdd = spark.read.csv("./data/quarter.csv").rdd
  private val rdd = spark.read.csv("gs://order-dataset/data/order_products.csv").rdd
  //private val rdd = rddRead.repartition(200)


  //Given an RDD[String] We'll parse all as (O, P) Where, O is the order and P is the product
  private val orderProductPair = rdd.map(e => {
    val tmp = e.toString().replace("[", "").replace("]", "").split(",")
    (tmp.head.toInt, tmp.tail.head.toInt)
  })

  //Now we can group by order to get a collection of pairs (O, P)
  private val orderAllProduct = orderProductPair.groupBy(e => {
    e._1
  })

  //Now we can get an RDD[List] Where every list is a set of products P in the same order. We do not need order ID anymore.
  private val onlyCompactBuffers = orderAllProduct.map(e => {
    e._2
  })
  //Now we can delete first key from allCompactBuffers and we'll get only RDDs with product ID.
  private val onlyProducts = onlyCompactBuffers.map(e => {
    e.map(pair => {
      pair._2
    })
  })

  //Now, for every pair Product ID RDD, we have to get all pairs.
  private val allProductPairs = onlyProducts.flatMap(productIds => {
    for {
      x <- productIds
      y <- productIds
      if x < y
    } yield (x, y)
  })

  private val result = allProductPairs.groupBy(e => e).map(e => {
    val (x, y) = e._1
    (x, y, e._2.size)
  })

  val df = spark.createDataFrame(result)
  df.write.format("csv").option("path", "gs://order-dataset/out/out-first.csv").save()
  clock.printElapsedTime()
}

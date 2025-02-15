package project

import org.apache.spark.sql.SparkSession
import project.utils.{Clock, OrderPartitioner}

import scala.collection.mutable.Map


object Third extends App {

  private val clock = new Clock()
  println("Hello! Scala is working")
  private val spark = SparkSession
    .builder()
    .appName("SCP")
    .master("local[*]")
    /*
    .config("spark.executor.memory", "4G")
    .config("spark.driver.memory", "4G")
    .config("spark.driver.maxResultSize", "4G")
    .config("spark.memory.offHeap.enabled", true)
    .config("spark.memory.offHeap.size", "4G")
     */
    .getOrCreate()
  println("Spark is running!")
  //private val rdd = spark.read.csv("./data/order_products.csv").rdd
  //private val rdd = spark.read.csv("./data/medium.csv").rdd
  private val filename = args.apply(0)
  private val rdd = spark.read.csv("gs://order-dataset/data/" + filename).rdd

  //Given an RDD[String] We'll parse all as (O, P) Where, O is the order and P is the product
  private val orderProductPair = rdd.map(e => {
    val tmp = e.toString().replace("[", "").replace("]", "").split(",")
    (tmp.head.toInt, tmp.tail.head.toInt)
  })

  private val partitioned = orderProductPair.partitionBy(new OrderPartitioner(96))
  val test = partitioned.groupBy(e => e._1).map(e => e._2).map(e => {
    e.map(pair => {
      pair._2
    })
  }).map(productIds => {
    for {
      x <- productIds
      y <- productIds
      if x < y
    } yield (x, y)
  }).map(list => {
    val mutable: Map[(Int, Int), Int] = Map()
    list.foldLeft(mutable) { (acc, element) =>
      acc.put(element, 1)
      acc
    }
  }).reduce((map1, map2) => {
    val notUnique = map1.keySet intersect map2.keySet
    val mapOut = map1 ++ map2
    notUnique.foreach(
      k => {
        val m1val = map1.getOrElse(k, 0)
        val m2val = map2.getOrElse(k, 0)
        mapOut.put(k, m1val + m2val)
      })
    mapOut
  }).map(tuple => {
    val (pair, value) = tuple
    val (x, y) = pair
    (x, y, value)
  })


  val df = spark.createDataFrame(test.toList).repartition(1)
  //df.write.format("csv").option("path", "gs://order-dataset/out/out-third-" + filename).save()
  //df.write.format("csv").option("path", "gs://order-dataset/out/out-third-" + filename).save()
  clock.printElapsedTime()
}

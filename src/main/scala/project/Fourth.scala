package project

import org.apache.spark.sql.SparkSession
import project.utils.{Clock, OrderPartitioner}

import scala.::
import scala.collection.mutable.{ListBuffer, Map}


object Fourth extends App {

  private val clock = new Clock()
  println("Hello! Scala is working")
  private val spark = SparkSession
    .builder()
    .appName("SCP") //TODO: RECOMMENT BELLOW
    /*
    .master("local[*]")
    .config("spark.executor.memory", "4G")
    .config("spark.driver.memory", "4G")
    .config("spark.driver.maxResultSize", "4G")
    .config("spark.memory.offHeap.enabled", true)
    .config("spark.memory.offHeap.size", "4G")
     */
    .getOrCreate()
  println("Spark is running!")
  //private val rdd = spark.read.csv("./data/order_products.csv").rdd
  private val rdd = spark.read.csv("./data/medium.csv").rdd
  //private val rdd = spark.read.csv("gs://order-dataset/data/order_products.csv").rdd

  //Given an RDD[String] We'll parse all as (O, P) Where, O is the order and P is the product
  private val orderProductPair = rdd.map(e => {
    val tmp = e.toString().replace("[", "").replace("]", "").split(",")
    (tmp.head.toInt, tmp.tail.head.toInt)
  })

  private val partitioned = orderProductPair.partitionBy(new OrderPartitioner(100))
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
    list.groupBy(e => e).map(group => {
      val (x, y) = group._1
      (x, y, 1)
    })
  }).mapPartitions(iterator => {
    var giantList = ListBuffer[(Int, Int, Int)]()
    while (iterator.hasNext)
      giantList = giantList ++ iterator.next()
    giantList.groupBy(e => (e._1, e._2)).map(group => {
      val (x, y) = group._1
      (x, y, group._2.size)
    }).toIterator
  }).groupBy(e => (e._1, e._2)).map(e => {
    val (x, y) = e._1
    val count = e._2.reduce((t1, t2) => {
      (0, 0, t1._3 + t2._3)
    })
    if(count._3 > 1) {
      val c = 1
    }
    (x, y, count._3)
  })


  println(test.collect().toList)

  val df = spark.createDataFrame(test).repartition(1)
  df.write.format("csv").option("path", "gs://order-dataset/out/out-second-thirty-second.csv").save()
  //df.write.format("csv").option("path", "out.csv").save()
  clock.printElapsedTime()
}

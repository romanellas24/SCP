import org.apache.spark.sql.SparkSession

object First extends App {
  println("Hello! Scala is working")
  private val spark = SparkSession.builder().appName("Hello").master("local[*]").getOrCreate()
  println("Spark is running!")
  //private val df = spark.read.format("csv").option("header", "true").load("/home/romanellas/Documents/order_products.csv/order_products.csv")
  //private val df = spark.read.format("csv").option("header", "true").load("/home/romanellas/Documents/order_products.csv/small.csv")
  //val rdd = spark.read.csv("/home/romanellas/Documents/order_products.csv/small.csv").rdd
  val rdd = spark.read.csv("/home/romanellas/Documents/order_products.csv/medium.csv").rdd
  val pairs = rdd.map(e => {
    val tmp = e.toString().replace("[", "").replace("]", "").split(",")
    (tmp.head.toInt, tmp.tail.head.toInt)
  })
  val parsed = pairs.groupBy(e => {
    e._1
  })
  println(parsed.collect().take(0).toList)

}

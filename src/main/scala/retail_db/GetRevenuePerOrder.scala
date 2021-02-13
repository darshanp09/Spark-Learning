package retail_db
import  org.apache.spark.{SparkContext,SparkConf}
object GetRevenuePerOrder {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster(args(0))
      .setAppName("Get Revenue Per Order")

    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    val orderItem = sc.textFile(args(1))
    val revenuePerOrder = orderItem.map(oi =>
      (oi.split(",")(1).toInt
      ,oi.split(",")(4).toFloat)
    ).reduceByKey(_ + _).
      map(oi => oi._1 +","+oi._2)

    revenuePerOrder.saveAsTextFile(args(3))


  }

}

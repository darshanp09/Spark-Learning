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

    revenuePerOrder.saveAsTextFile(args(2))

//    sbt "run-main ClassName args"

    /*
     spark-submit --class retail_db.GetRevenuePerOrder "/Users/swaminarayan/Documents/Spark/spark/spark-demo/target/scala-2.11/spark-demo_2.11-0.1.jar" local /Users/swaminarayan/Documents/data/retail_db/order_items /Users/swaminarayan/Documents/Spark/spark/spark-demo/revenue_per_order


spark-submit \
--class retail_db.GetRevenuePerOrder \
--master local \
--deploy-mode client \
--conf spark.ui.port=12678 \
spark-demo_2.11-0.1.jar \
yarn-client /mnt/home/edureka_555589/public/part-00000 /mnt/home/edureka_555589/public
     */

  }

}

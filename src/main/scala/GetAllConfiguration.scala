import org.apache.spark.{SparkContext,SparkConf}

object GetAllConfiguration {

  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().
      setAppName("Get All Configuration").
      setMaster("local").
      set("spark.ui.port","12678")

    conf.getAll.foreach(println)

    val sc = new SparkContext(conf);
  }

}

package retail_db

object Learning {

  def main(args: Array[String]): Unit = {
    val orderItemPath = args(0)

    val orderId = args(1).toInt

    val orderItems = scala.io.Source.fromFile(orderItemPath).getLines().toList

//    orderItems.take(10).foreach(println)
    val orderItemsFiltered = orderItems.filter( i => i.split(",")(1).toInt == orderId)

    val subTotal = orderItemsFiltered.map( i => i.split(",")(4).toFloat).reduce((x,y)=>x+y)

    println("Order revenue for orderId "+orderId+" is "+subTotal);
  }

}

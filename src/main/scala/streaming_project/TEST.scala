package streaming_project

object TEST {
  def main(args: Array[String]): Unit = {
    val list = List( ("张三", 10), ("李思思", 50), ("ww", 20) )

    list.foreach( x => {
      println(s"name: ${x._1}, age: ${x._2}")
    })

    // 等价于上面的方法， 相当于取别名
    list.foreach{
      case (name, age) => {
        println(s"name: ${name}, age: ${age}")
      }
    }

  }
}

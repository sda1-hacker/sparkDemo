package spark_sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


// case class Person(name: String, age: String, gender: String){}
case class Person(){
  var name: String = _
  var age: String = _
  var gender: String = _

}

object DataFrameTest {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .appName("sql")
      .master("local[*]")
      .getOrCreate()

    val context = sparkSession.sparkContext

    import sparkSession.implicits._   // 隐式类，sparkSession 就是上面定义的

    val dataFrame = List(
      ("张三", 90),
      ("张三", 91),
      ("张三", 92),
      ("赵柳柳", 93),
      ("赵柳柳", 94),
      ("赵柳柳", 95)
    ).toDF("name", "salary")

    // 定义视图
    dataFrame.createOrReplaceTempView("t_user")

    var sql =
      """
        |select name, salary,
        | sum(salary) over(partition by name ) as a1,
        | sum(salary) over(partition by name order by salary desc) as a2
        | from t_user
        |""".stripMargin

    sparkSession.sql(sql).show()

  }
}

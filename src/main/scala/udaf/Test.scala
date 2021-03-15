package udaf

import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}

object Test {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .appName("sql")
      .master("local[*]")
      .getOrCreate()

    val context = sparkSession.sparkContext

    import sparkSession.implicits._   // 隐式类，sparkSession 就是上面定义的

    val dataFrame = List(
      ("张三", 16, "三年二班"),
      ("李思思", 17, "五年一班"),
      ("赵柳柳", 19, "三年三班"),
      ("周奇奇", 20, "八年年五班")
    ).toDF("name", "age", "classFrom")

    // mysql配置
    val props = new Properties()
    props.put("user", "root")
    props.put("password", "123456")

    // 写入mysql
    dataFrame.write.jdbc("jdbc:mysql://baseCentOS:3306/test1", "df_user", props)

    // 从mysql中读取
    sparkSession.read.jdbc("jdbc:mysql://baseCentOS:3306/test1", "df_user", props).show()
  }
}

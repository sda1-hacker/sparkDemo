package structured_streaming

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.sql.{ForeachWriter, Row}


class MysqlWriter extends ForeachWriter[Row]{

  var conn: Connection = _
  var ps: PreparedStatement = _

  // 打开连接
  override def open(partitionId: Long, epochId: Long): Boolean = {
    conn = DriverManager.getConnection("jdbc:mysql://baseCentOS:3306/spark", "root", "123456")
    var sql = // 这里用word作为主键了
      """
        |insert into t_word(word, num) values(?, ?) on duplicate key update word=?, num=?
        |""".stripMargin
    ps = conn.prepareStatement(sql)
    !conn.isClosed && !ps.isClosed
  }

  // 执行操作 -- open 返回 true的时候执行这个方法
  override def process(value: Row): Unit = {
    val word = value.getAs[String](0)
    val num = value.getAs[Long](1)
    ps.setString(1, word)
    ps.setLong(2, num)

    ps.setString(3, word)
    ps.setLong(4, num)

    ps.executeUpdate()
  }

  // 关闭连接
  override def close(errorOrNull: Throwable): Unit = {
    ps.close()
    conn.close()
  }
}

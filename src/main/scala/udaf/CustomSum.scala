package udaf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, StructType}

class CustomSum extends UserDefinedAggregateFunction{

  // 聚合函数输入参数的类型以及个数
  override def inputSchema: StructType = {
    new StructType()
      .add("personAge", IntegerType)
      // .add("personNum", IntegerType)  // 多个参数调用add方法添加即可
  }

  // 缓冲区中定义的变量
  override def bufferSchema: StructType = {
    new StructType()
      .add("ageSum", IntegerType)
      .add("personNum", IntegerType)   // 多个参数调用add方法添加即可
  }

  // 返回值的数据类型以及个数
  override def dataType: DataType = DoubleType

  // 对于相同的输入是否返回相容的输出
  override def deterministic: Boolean = true

  // 对缓冲区中定义的变量进行初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0
    buffer(1) = 0
  }

  // 更新缓冲区的数据 -- 分区内的合并
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val age1 = buffer.getAs[Int](0)   // 缓冲区的数据
    val age2 = input.getAs[Int](0)    // 输入的数据
    buffer.update(0, age1 + age2)

    val personNum = buffer.getAs[Int](1)
    buffer.update(1, personNum + 1)
  }

  // 合并缓冲区的数据  -- 分区之间的合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val age1 = buffer1.getAs[Int](0)
    val age2 = buffer2.getAs[Int](0)
    buffer1.update(0, age1 + age2)

    val personNum1 = buffer1.getAs[Int](1)
    val personNum2 = buffer2.getAs[Int](1)
    buffer1.update(1, personNum1 + personNum2)
  }

  // 返回最终结果
  override def evaluate(buffer: Row): Any = {
    (1.0 * buffer.getAs[Int](0)) / buffer.getAs[Int](1)
  }
}

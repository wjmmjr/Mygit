import org.apache.spark.sql.types.{StringType, StructField}

/**
  * Created by X.Y on 2018/6/29
  */
object test {

  def main(args: Array[String]): Unit = {
    val schemaString = "name age"

    // Generate the schema based on the string of schema   Array[StructFiled]
    //一个StructField就像一个SQL中的一个字段一样，它包含了這个字段的具体信息
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName,StringType))

  }

}

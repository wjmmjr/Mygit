import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
  * Created by X.Y on 2018/6/23
  */
object test {

  def main(args: Array[String]): Unit = {
    // 创建Spark配置
    val sparkConf = new SparkConf().setAppName("MockData").setMaster("local[*]")

    // 创建Spark SQL 客户端
    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    // 加载SparkSQL的隐式转换支持
    import spark.implicits._
    val countMap =new mutable.HashMap[String,Int]()
    countMap.put("k",3)
    println(countMap("k"))
    countMap("k")=countMap("k")+1
    println(countMap("k"))

    spark.stop()

  }


}

import org.apache.spark.util.AccumulatorV2
import scala.collection._
/**
  * Created by X.Y on 2018/6/25
  * 自定义累加器
  */
class SessionStatAccumulator extends AccumulatorV2[String,mutable.HashMap[String,Int]]{

  val countMap =new mutable.HashMap[String,Int]()

  override def isZero: Boolean = countMap.isEmpty

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Int]] = {
    val acc=new SessionStatAccumulator
    acc.countMap ++=this.countMap
    acc
  }

  override def reset(): Unit = countMap.clear()

  override def add(v: String): Unit = {
    //判断countMap是否存在key值为v的键值对
    if (!countMap.contains(v)){
      countMap+=(v->0)
    }
   // countMap(v)=countMap(v)+1
    countMap.update(v,countMap(v)+1)
  }

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = {
    other match {
      case acc:SessionStatAccumulator =>acc.countMap.foldLeft(this.countMap){
        case(map,(k,v))=>map += (k->(map.getOrElse(k,0)+v))
      }
    }
  }

  override def value: mutable.HashMap[String, Int] = {this.countMap}
}

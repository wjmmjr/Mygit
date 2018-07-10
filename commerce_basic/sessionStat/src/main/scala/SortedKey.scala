/**
  * Created by X.Y on 2018/6/26
  */
case class SortedKey(clickCount: Long, orderCount: Long, payCount: Long)extends Ordered[SortedKey]{
  override def compare(that: SortedKey): Int = {
    if(this.clickCount-that.clickCount!=0){
      return (this.clickCount-that.clickCount).toInt
    } else if (this.orderCount - that.orderCount != 0) {
      return (this.orderCount - that.orderCount).toInt
    } else if (this.payCount - that.payCount != 0) {
      return (this.payCount - that.payCount).toInt
    }
    0
  }
}

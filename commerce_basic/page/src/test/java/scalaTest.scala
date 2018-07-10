import Worker.worker

/**
  * Created by X.Y on 2018/6/28
  */
object scalaTest{
  def main(args: Array[String]) {
    /*  Worker.GetWorkInstance()
    var worker = new Worker*/
    def foo(msg : String) = {
     msg
    }
    implicit def intToString(x:Int) = x.toString
    var a=foo("12")
    print(a+1)



  }
}

 class  Worker {
    private def work= println("I am the only worker!")
  }

  object Worker{
    var worker:Worker = null
    def GetWorkInstance() : Unit = {
      if(worker==null){
        worker=new Worker
      }
      worker.work
    }

  }


object TrafficLightColor extends Enumeration {
  val Red = Value(0, "Stop")
  val Yellow = Value(1, "Slow")
  val Green = Value(2, "Go")
}


trait Logger {
  def log(msg: String)  // 抽象方法
  def log2(msg: String): Unit ={
    println("12344")
  }
}


//==================


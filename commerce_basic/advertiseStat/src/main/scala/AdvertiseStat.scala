
import java.util.Date

import commons.conf.ConfigurationManager
import commons.utils.DateUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.collection.mutable.ArrayBuffer
/**
  * Created by X.Y on 2018/6/29
  */
object AdvertiseStat {



  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("adver").setMaster("local[*]")

    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    // val streamingContext =StreamingContext.getActiveOrCreate(checkPointPath, Func)
    // 优先从checkpointPath去反序列化回我们的上下文数据
    // 如果没有相应的checkpointPath对应的文件夹，就调用Func去创建新的streamingcontext
    // 如果有Path，但是代码修改过，直接报错，删除原来的Path内容，再次启动

    // offset写入checkpoint，与此同时，还保存一份到zookeeper
    val streamingContext = new StreamingContext(sparkSession.sparkContext, Seconds(5))

    val kafka_brokers = ConfigurationManager.config.getString("kafka.broker.list")
    val kafka_topics = ConfigurationManager.config.getString("kafka.topics")

    val kafkaParam=Map(

    "bootstrap.servers" -> kafka_brokers,
    "key.deserializer" -> classOf[StringDeserializer],//key的解码器
    "value.deserializer" -> classOf[StringDeserializer],//value的解码器
    "group.id" -> "test-consumer-group",
    // largest最大   smallest最小   none没有  ->老版
    // latest最新    earilst最早    none没有  ->新版
    // latest:   在消费指定topic的分区时，如果已经有offset，直接使用之前保存的offset，
    //           如果没有offset，就从最新的数据开始消费
    // earilist: 在消费指定topic的分区时，如果已经有offset，直接使用之前保存的offset，
    //           如果没有offset，就从最早的数据开始消费
    // none:    在消费指定topic的分区时，如果已经有offset，直接使用之前保存的offset，
    //           如果没有offset，直接报错
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false:java.lang.Boolean)//不设置自动提交
    )
    //生成DirectStream
    val adRealTimeDataDStream =KafkaUtils.createDirectStream(streamingContext,
    LocationStrategies.PreferConsistent,//定位策略
    ConsumerStrategies.Subscribe[String,String](Array(kafka_topics),kafkaParam)//
    )
    val adRealTimeValueDStream = adRealTimeDataDStream.map{item => item.value()}
    // 根据黑名单的userId去过滤数据，(userid, timestamp province city userid adid)
    val adRealTimeFilteredDStream = filterBlackList(sparkSession, adRealTimeValueDStream)
    // 需求七：实时维护黑名单
    generateBlackListOnTime(adRealTimeFilteredDStream)
    // 设置检查点目录
    streamingContext.checkpoint("./spark_streaming")
    // 需求八：各省各城市广告点击量实时统计
    val adRealTimeStatDStream = calculateRealTimeStat(adRealTimeFilteredDStream)
    // 需求九：统计每个省Top3热门广告
    getProvinceTop3Ad(sparkSession,adRealTimeStatDStream)
    // 需求十：统计一个小时内每个广告每分钟的点击次数
    getPerHourPerMinuteClickCount(adRealTimeFilteredDStream)

    streamingContext.start()

    streamingContext.awaitTermination()
  }



  def getPerHourPerMinuteClickCount(adRealTimeFilteredDStream: DStream[(Long, String)]) = {
    val key2MinuteNum =adRealTimeFilteredDStream.map{
      case (userId, log) =>
        val timeStamp = log.split(" ")(0).toLong
        val date = new Date(timeStamp)
        // yyyyMMddHHmm
        val minute = DateUtils.formatTimeMinute(date)
        val adid = log.split(" ")(4).toLong

        val key=minute + "_" + adid
        (key,1L)
    }
    //滑动窗口统计一小时内的每一分钟点击量,窗口60分钟，滑步1分钟。reduceByKeyAndWindow是有状态变化，需要提前使用checkpoint
    val key2HourMinuteDStream = key2MinuteNum.reduceByKeyAndWindow((a:Long,b:Long)=>a+b,Minutes(60),Minutes(1))
    key2HourMinuteDStream.foreachRDD{
      rdd=>
        rdd.foreachPartition{
          items=>
            val clickTrendArray = new ArrayBuffer[AdClickTrend]()
            for(item<-items){
              val keySplit =item._1.split("_")
              // yyyyMMddHHmm
              val minute = keySplit(0)
              val date = minute.substring(0, 8)
              val hour = minute.substring(8, 10)
              val minu = minute.substring(10)
              val adid=keySplit(1).toLong
              val count = item._2

              clickTrendArray += AdClickTrend(date, hour, minu, adid, count)
            }
            AdClickTrendDAO.updateBatch(clickTrendArray.toArray)
        }
    }


  }




  def getProvinceTop3Ad(sparkSession: SparkSession,  adRealTimeStatDStream: DStream[(String, Long)]) ={
    val provinceTop3AdDStream =adRealTimeStatDStream.transform{
      //key2CountRDD:(datekey_province_city_adid,count)
      key2CountRDD=>
        val key2ProvinceCountRDD =key2CountRDD.map{
          case(keyid,count)=>
           val keySplit=keyid.split("_")
            val dateKey = keySplit(0)
            val province = keySplit(1)
            val adid = keySplit(3).toLong
            val newKey = dateKey + "_" + province + "_" + adid
            (newKey, count)
        }
        import sparkSession.implicits._
        key2ProvinceCountRDD.reduceByKey(_+_).map{
          case (key, count) =>
            val keySplit = key.split("_")
            val dateKey = keySplit(0)
            val province = keySplit(1)
            val adid = keySplit(2).toLong
            (dateKey, province, adid, count)
        }.toDF("date", "province", "adid", "count").createOrReplaceTempView("tmp_province_ad_count")//创建临时表
      //查询临时表
      val sql = "select date, province, adid, count from(" +
        " select date, province, adid, count," +
        "row_number() over(partition by province order by count desc) rank from tmp_province_ad_count) t" +
        " where rank<=3"
        val resultMap=sparkSession.sql(sql).rdd
        resultMap
    }
    provinceTop3AdDStream.foreachRDD{rdd=>
      rdd.foreachPartition{items=>
        // Array[AdProvinceTop3]
        val top3Array = new ArrayBuffer[AdProvinceTop3]()
        // item : Row -> date, province, adid, count
        for(item <- items){
          val date = item.getAs[String]("date")
          val province = item.getAs[String]("province")
          val adid = item.getAs[Long]("adid")
          val count = item.getAs[Long]("count")

          top3Array += AdProvinceTop3(date, province, adid, count)
        }

        AdProvinceTop3DAO.updateBatch(top3Array.toArray)
      }

    }
  }

  // 计算每天各省各城市各广告的点击量
  def calculateRealTimeStat(adRealTimeFilteredDStream: DStream[(Long, String)]) = {
    // adRealTimeFilteredDStream =>(userid, timestamp province city userid adid)
    //以“时间-省-城市-广告iD”为key
    val mappedDStream =adRealTimeFilteredDStream.map{case(userid,log)=>
      val logSplited = log.split(" ")
      val timestamp = logSplited(0)
      val date = new Date(timestamp.toLong)
      val datekey = DateUtils.formatDateKey(date)

      val province = logSplited(1)
      val city = logSplited(2)
      val adid = logSplited(4).toLong

      val key = datekey + "_" + province + "_" + city + "_" + adid

      (key, 1L)
    }
    //用updateStateByKey对(key, 1L)聚合操作，因为是实时数据，数据在不断更新叠加，将叠加后的最新统计数据直接写到数据库
    /*用updateStateByKey不用reduceByKey的话，当前批次数据会与历史数据进行累加，后台保存时不需要sql查询出来每条数据的count
      再与当前count累加保存,这样提高了效率*/
    //对状态(state)的DStream操作(updateStateByKey)，操作会跨多个batch duration，后面数据对前面的有依赖，随着时间的推移，依赖链条会越来越长，这个时候需要使用checkpoint
    val aggregatedDStream =mappedDStream.updateStateByKey[Long]{(values:Seq[Long], old:Option[Long]) =>
      var clickCount = 0L
      // 若历史数据存在，那么就以之前的状态作为起点，进行值的累加
      if(old.isDefined){
        clickCount=old.get
      }
      for(value<-values){
        clickCount+=value
      }
      Some(clickCount)
    }
//将数据封装到样例类写进数据库
    aggregatedDStream.foreachRDD{rdd=>
      rdd.foreachPartition{items=>
        //批量保存到数据库
        val adStats=ArrayBuffer[AdStat]()
        for(item <- items){
          val keySplited = item._1.split("_")
          val date = keySplited(0)
          val province = keySplited(1)
          val city = keySplited(2)
          val adid = keySplited(3).toLong

          val clickCount = item._2
          adStats += AdStat(date,province,city,adid,clickCount)
        }
      AdStatDAO.updateBatch(adStats.toArray)
      }

    }
    aggregatedDStream
  }



  def generateBlackListOnTime(adRealTimeFilteredDStream: DStream[(Long, String)]): Unit = {
    val key2NumDStream =adRealTimeFilteredDStream.map{
      // log: timestamp province city  userid adid
      case(userId,log)=>
        val logSplit=log.split(" ")
        val timeStamp=logSplit(0).toLong
        val date = new Date(timeStamp)
        val dateKey =DateUtils.formatDate(date)
        val adid=logSplit(4).toLong
        val key=dateKey+"_"+userId+"_"+adid
        (key,1L)

    }
    // 一天中，每一个用户对于某一个广告的点击次数
    /*用reduceByKey不用updateStateByKey的话，只是累加当前批次数据，后台保存时需要sql查询出来每条数据的count再与当前count累加
    保存,这样会降低效率*/
    val key2CountDStream =key2NumDStream.reduceByKey(_+_)
     key2CountDStream.foreachRDD{
       rdd=>rdd.foreachPartition{
            iterm=>
              val clickCountArray=new ArrayBuffer[AdUserClickCount]()
              for(it<-iterm){
                val keySplit=it._1.split("_")
                val dateKey = keySplit(0)
                val userId = keySplit(1).toLong
                val adid = keySplit(2).toLong
                val count = it._2
                clickCountArray +=AdUserClickCount(dateKey, userId, adid, count)
              }
              AdUserClickCountDAO.updateBatch(clickCountArray.toArray)
          }
     }
    // 获取最新的用户点击次数表里的数据，然后过滤出所有要加入到黑名单里面的用户ID
    val userIdBlackListDStream =key2CountDStream.filter{
      case(key,count)=>
        val keySplit = key.split("_")
        val dateKey = keySplit(0)
        val userId = keySplit(1).toLong
        val adid = keySplit(2).toLong
        val count =AdUserClickCountDAO.findClickCountByMultiKey(dateKey, userId, adid)
        if(count >= 100){
          true
        }else{
          false
        }
    }.map{
      case (key,count)=>
      val userId = key.split("_")(1).toLong
      userId
    }.transform(rdd=>rdd.distinct())

    userIdBlackListDStream.foreachRDD{
      rdd=>rdd.foreachPartition{items=>
        val blackListArray = new ArrayBuffer[AdBlacklist]()
        for(item <- items){
          blackListArray += AdBlacklist(item)
        }
        AdBlacklistDAO.insertBatch(blackListArray.toArray)
      }

    }


  }



  def filterBlackList(sparkSession: SparkSession, adRealTimeValueDStream: DStream[String]) = {

    // recordRDD: RDD[String] timestamp 	  province 	  city        userid         adid
    adRealTimeValueDStream.transform { recordRDD =>

      //blackListUserInfoArray: Array[AdBlacklist]
      // AdBlacklist: case class AdBlacklist(userid:Long)
      val blackListUserInfoArray = AdBlacklistDAO.findAll()
      // RDD[AdBlacklist]
      val blackListUserRDD = sparkSession.sparkContext.makeRDD(blackListUserInfoArray).map{
        // AdBlacklist
        item => (item.userid, true)
      }

      val userId2RecordRDD = recordRDD.map{
        // item : String -> timestamp 	  province 	  city        userid         adid
        item => val userId = item.split(" ")(3).toLong
          (userId, item)
      }

      val userId2FilteredRDD = userId2RecordRDD.leftOuterJoin(blackListUserRDD).filter{
        case (userId,(record, bool)) =>
          if(bool.isDefined && bool.get){
            false
          }else{
            true
          }
      }.map{
        case (userId, (record, bool))=>(userId, record)
      }

      userId2FilteredRDD
    }

  }

}

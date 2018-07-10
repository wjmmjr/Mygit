

import java.util.{Date, Random, UUID}

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.utils._
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import commons.model._
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
/**
  * Created by X.Y on 2018/6/25
  */
object SessionStat {

  def main(args: Array[String]): Unit = {

    val jsonStr=ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskParam=JSONObject.fromObject(jsonStr)

    val sparkConf=new SparkConf().setAppName("SessionStat").setMaster("local[*]")
    val sparkSession =SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val actionRDD = getBasicActionData(sparkSession, taskParam)
    //actionRDD.foreach(println(_))
    //转化为K-V结构，以SessionId为key
    val sessionId2ActionRDD =actionRDD.map(item=>(item.session_id,item))
    val sessionId2GroupRDD = sessionId2ActionRDD.groupByKey()

    sessionId2GroupRDD.cache()
    val sessionId2FullInfoRDD = getSessionFullInfo(sparkSession, sessionId2GroupRDD)

   // sessionId2FullInfoRDD.foreach(println(_))
    //注册自定义累加器
    val sessionStatAccumulator=new SessionStatAccumulator
    sparkSession.sparkContext.register(sessionStatAccumulator)
    //按照年龄、职业、城市范围、性别、搜索词、点击品类这些条件过滤后
    val filteredSessionid2AggrInfoRDD = filterSessionAndAggrStat(sessionId2FullInfoRDD, taskParam, sessionStatAccumulator)
    //filteredSessionid2AggrInfoRDD.foreach(println(_))//随便执行下Action操作，触发RDD的计算
    // 需求一：各范围session占比统计
        // 创建UUID
        val taskUUID = UUID.randomUUID().toString
    getSessionRatio(sparkSession, taskUUID, sessionStatAccumulator.value)
    //需求二：随机抽取session
    sessionRandomExtract(sparkSession, taskUUID,sessionId2FullInfoRDD)
    //获取符合过滤条件的所有原始session
    val sessionid2detailRDD = sessionId2ActionRDD.join(filteredSessionid2AggrInfoRDD).map{
      case(sessionid,(orValue,filterValue))=>(sessionid,orValue)
    }
    //需求三：获取top10热门品类，在Top10的排序中，按照点击数量、下单数量、支付数量的次序进行排序，即优先考虑点击数量。
    val top10CategoryList = getTop10Category(sparkSession, taskUUID, sessionid2detailRDD)
    // 需求四：统计每一个Top10热门品类的Top10活跃session
    getTop10ActiveSession(sparkSession, taskUUID, top10CategoryList, sessionid2detailRDD)


  }


  def getTop10ActiveSession(sparkSession: SparkSession, taskUUID: String, top10CategoryList: Array[(SortedKey, String)],
                            sessionid2detailRDD: RDD[(String, UserVisitAction)]) = {
    //获取Top10的cid
    val top10CategoryRDD = sparkSession.sparkContext.makeRDD(top10CategoryList).map{
      case(sortKey,fullInfo)=>
        val cid=StringUtils.getFieldFromConcatString(fullInfo,"\\|",Constants.FIELD_CATEGORY_ID).toLong
        (cid,cid)
    }

    //把符合过滤条件的所有全数据更改为k为cid的数据
    val cid2ActionRDD =sessionid2detailRDD.map{
      case (sid, action) =>
        val cid=action.click_category_id
        (cid,action)
    }
//Top10品类的所有对应session
    val sessionId2FilteredRDD =top10CategoryRDD.join(cid2ActionRDD).map{
      case(cid,(categoryId,action))=>
        val sessionId=action.session_id
        (sessionId,action)
    }

    // 第二步：统计每个session对Top10热门品类的点击次数
      //在一个(K,V)的RDD上调用，返回一个(K, Iterator[V])的RDD
    val sessionId2GroupRDD = sessionId2FilteredRDD.groupByKey()
    val cid2SessionCountRDD = sessionId2GroupRDD.flatMap{
      case (sessionId, iterableAction) =>
        val categoryCountMap = new mutable.HashMap[Long, Long]()
        for(action <- iterableAction){
          val categoryId = action.click_category_id
          if(!categoryCountMap.contains(categoryId))
            categoryCountMap += (categoryId->0)
          categoryCountMap.update(categoryId, categoryCountMap(categoryId) +  1)

        }
        for((cid, count) <- categoryCountMap)
          yield (cid, sessionId + "=" + count)//将遍历过程中处理的结果返回到一个新的集合中，使用yield关键字
    }
    val cid2GroupSessionCountRDD = cid2SessionCountRDD.groupByKey()
    val top10SessionRDD = cid2GroupSessionCountRDD.flatMap{
      // iterableSessionCount是一个iterable类型，里面是String，每一个String都是sessionid=count
      case (cid, iterableSessionCount) =>
        val sortedSession  =iterableSessionCount.toList.sortWith((item1,item2)=>
          item1.split("=")(1).toLong>item2.split("=")(1).toLong
        )
        //取每个品类的sessionId最多的10条数据
        val top10Session =sortedSession.take(10)
        val top10SessionCaseClass =top10Session.map{
          item=>
            val sessionid = item.split("=")(0)
            val count = item.split("=")(1).toLong
            Top10Session(taskUUID,cid,sessionid,count)
        }
        top10SessionCaseClass
    }
    import sparkSession.implicits._
    top10SessionRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "top10_session0115")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()
  }






  def getTop10Category(sparkSession: SparkSession, taskUUID: String, sessionid2detailRDD: RDD[(String, UserVisitAction)]) = {

    // 获取所有产生过点击、下单、支付中任意行为的商品类别
   var categoryidRDD= sessionid2detailRDD.flatMap{
      case(sessionid, userVisitAction) =>
        var arr=ArrayBuffer[(Long,Long)]()
        if(userVisitAction.click_category_id != -1L){// 一个session中点击的商品ID;造数据的时候，clickCategoryId初始值是-1L
          val clickId=userVisitAction.click_category_id
          arr +=((clickId,clickId))
        }else if(userVisitAction.order_category_ids!=null){// 一个session中下单的商品ID集合
          val orderIds=userVisitAction.order_category_ids.split(",")
          for(orderId<-orderIds){
            arr +=((orderId.toLong,orderId.toLong))
          }
      }else if(userVisitAction.pay_category_ids!=null){// 一个session中支付的商品ID集合
          val payIds=userVisitAction.pay_category_ids.split(",")
          for(payId<-payIds){
            arr +=((payId.toLong,payId.toLong))
          }
        }
        arr
    }
    //去重，一个sessionId认为是一个用户进行访问
    categoryidRDD=categoryidRDD.distinct()

  //计算各品类的点击、下单和支付的次数
    // 统计每一个被点击的品类的点击次数（cid, count）
    val clickCount = getCategoryClickCount(sessionid2detailRDD)

    // 统计每一个被下单的品类的下单次数
    val orderCount = getCategoryOrderCount(sessionid2detailRDD)

    // 统计每一个被付款的品类的付款次数
    val payCount = getCategoryPayCount(sessionid2detailRDD)

    // 合并(cid, fullInfo(cid|clickCount|orderCount|payCount))
    val cid2FullInfo = getFullInfoCount(categoryidRDD, clickCount, orderCount, payCount)
//根据点击、下单、付款排序
 val sortKey2FullInfo = cid2FullInfo.map{
      case(cid,fullInfo)=>
        val clickCount=StringUtils.getFieldFromConcatString(fullInfo,"\\|",Constants.FIELD_CLICK_COUNT).toLong
        val orderCount = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_ORDER_COUNT).toLong
        val payCount = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_PAY_COUNT).toLong
        val sortKey = new SortedKey(clickCount, orderCount, payCount)
        (sortKey, fullInfo)
    }
    val top10Category = sortKey2FullInfo.sortByKey(false).take(10)

    val top10CategoryRDD = sparkSession.sparkContext.makeRDD(top10Category)

    val top10CategoryWriteRDD = top10CategoryRDD.map{
      case (sortedKey, fullInfo) =>
        val categoryId = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_CATEGORY_ID).toLong
        val clickCount = sortedKey.clickCount
        val orderCount = sortedKey.orderCount
        val payCount = sortedKey.payCount

        Top10Category(taskUUID, categoryId, clickCount, orderCount, payCount)
    }

    import sparkSession.implicits._
    top10CategoryWriteRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "top10_category0115")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()
    top10Category
  }


  def getCategoryClickCount(sessionid2detailRDD: RDD[(String, UserVisitAction)]) = {
     val sessionId2FilterRDD = sessionid2detailRDD.filter{
       case(sessionid, userVisitAction) =>userVisitAction.click_category_id!=null
      }
    val cid2NumRDD=sessionId2FilterRDD.map{
      case(sessionid, userVisitAction) =>(userVisitAction.click_category_id,1L)
    }
     cid2NumRDD.reduceByKey(_+_)
  }

  def getCategoryOrderCount(sessionid2detailRDD: RDD[(String, UserVisitAction)]) ={
    val sessionId2FilterRDD = sessionid2detailRDD.filter{
      case(sessionid, userVisitAction) =>userVisitAction.order_category_ids!=null
    }
    val cid2NumRDD=sessionId2FilterRDD.flatMap{
      case(sessionid, userVisitAction) =>
        userVisitAction.order_category_ids.split(",").map(item=>(item.toLong,1L))
    }
    cid2NumRDD.reduceByKey(_+_)
  }

  def getCategoryPayCount(sessionid2detailRDD: RDD[(String, UserVisitAction)]) = {
    val sessionId2FilterRDD = sessionid2detailRDD.filter{
      case(sessionid, userVisitAction) =>userVisitAction.pay_category_ids!=null
    }
    val cid2NumRDD=sessionId2FilterRDD.flatMap{
      case(sessionid, userVisitAction) =>
        userVisitAction.pay_category_ids.split(",").map(item=>(item.toLong,1L))
    }
    cid2NumRDD.reduceByKey(_+_)
  }

  def getFullInfoCount(categoryidRDD: RDD[(Long, Long)], clickCount: RDD[(Long, Long)],
                       orderCount: RDD[(Long, Long)], payCount: RDD[(Long, Long)]) = {
    val clickCountRDD =categoryidRDD.leftOuterJoin(clickCount).map{
      case(cid,(cid2,cCount))=>
        val count=if(cCount.isDefined) cCount.get else 0L
        val clickInfo =Constants.FIELD_CATEGORY_ID +"="+cid+"|"+Constants.FIELD_CLICK_COUNT + "=" + count
        (cid,clickInfo)
    }
    val orderCountRDD = clickCountRDD.leftOuterJoin(orderCount).map{
      case (cid, (aggrInfo, oCount)) =>
        val count = if(oCount.isDefined) oCount.get else 0L
        val orderInfo = aggrInfo + "|" + Constants.FIELD_ORDER_COUNT + "=" + count
        (cid, orderInfo)
    }

    val payCountRDD = orderCountRDD.leftOuterJoin(payCount).map{
      case (cid, (aggrInfo, pCount)) =>
        val count = if(pCount.isDefined) pCount.get else 0L
        val payInfo = aggrInfo + "|" + Constants.FIELD_PAY_COUNT + "=" + count
        (cid, payInfo)
    }
    payCountRDD
  }





  def sessionRandomExtract(sparkSession: SparkSession,
                           taskUUID: String,
                           sessionId2FullInfoRDD: RDD[(String, String)]): Unit ={
    // 第一步，计算出每天每小时的session数量，获取<yyyy-MM-dd_HH,aggrInfo>格式的RDD
   val dateHour2FullInfo= sessionId2FullInfoRDD.map{
      case(sessionId,fullInfo)=>
        val startTime=StringUtils.getFieldFromConcatString(fullInfo,"\\|",Constants.FIELD_START_TIME)
        //日期格式化（yyyy-MM-dd_HH）
        val dateHour=DateUtils.getDateHour(startTime)
        (dateHour,fullInfo)
    }
//记录了每天的session个数,countByKey返回一个(K,Int)的map，表示每一个key对应的元素个数。
    val dateHourCountMap =dateHour2FullInfo.countByKey()
//嵌套Map结构：第一层key为date，第二层key为hour
    val dateHourSessionCountMap =new mutable.HashMap[String,mutable.HashMap[String,Long]]()
    for((dateHour,count)<-dateHourCountMap){
      val date=dateHour.split("_")(0)
      val hour=dateHour.split("_")(1)
      dateHourSessionCountMap.get(date) match{
        case None => dateHourSessionCountMap(date)=new mutable.HashMap[String,Long]
          dateHourSessionCountMap(date).put(hour,count)
        case Some(map)=>dateHourSessionCountMap(date).put(hour,count)
      }
    }
//每天需要抽取的数量
    val sessionExtractCountPerDay=100/dateHourSessionCountMap.size
    val dateHourRandomIndexMap=new mutable.HashMap[String,mutable.HashMap[String,mutable.ListBuffer[Int]]]
    val random = new Random()

    for((date, hourCountMap) <- dateHourSessionCountMap){
      //每天的session数量
      val dayCount = hourCountMap.values.sum
      dateHourRandomIndexMap.get(date) match{
        case None => dateHourRandomIndexMap(date)=new mutable.HashMap[String,mutable.ListBuffer[Int]]()
          generateRandomIndex(sessionExtractCountPerDay,dayCount,hourCountMap,dateHourRandomIndexMap(date))
        case Some(map) =>
          generateRandomIndex(sessionExtractCountPerDay, dayCount, hourCountMap, dateHourRandomIndexMap(date))
      }
    }
    def generateRandomIndex(sessionExtractCountPerDay: Int, dayCount: Long, hourCountMap: mutable.HashMap[String, Long],
                            hourIndexMap:mutable.HashMap[String, mutable.ListBuffer[Int]]) ={
       val random = new Random()
        //得到这一个小时要抽取多少个session
      for((hour,count)<-hourCountMap){
        var hourExtractSessionCount =((count/dayCount.toDouble)*sessionExtractCountPerDay).toInt
        if (hourExtractSessionCount>count.toInt){
          hourExtractSessionCount=count.toInt
        }
        hourIndexMap.get(hour) match {
          case None=>hourIndexMap(hour)=new mutable.ListBuffer[Int]
            for(i<-0 until hourExtractSessionCount){
              var index=random.nextInt(count.toInt)
              while(hourIndexMap(hour).contains(index)){
                index=random.nextInt(count.toInt)
              }
              hourIndexMap(hour) +=index
            }
          case Some(list) =>
            for(i <- 0 until hourExtractSessionCount){
              var index = random.nextInt(count.toInt)
              while(hourIndexMap(hour).contains(index)){
                index = random.nextInt(count.toInt)
              }
              hourIndexMap(hour) += index
            }
        }
      }
    }
    // 执行groupByKey算子，得到<yyyy-MM-dd_HH,(session aggrInfo)>
    val time2sessionsRDD = dateHour2FullInfo.groupByKey()
    val sessionRandomExtract =time2sessionsRDD.flatMap{
      case (dateHour, items) =>
        val date = dateHour.split("_")(0)
        val hour = dateHour.split("_")(1)
        val extractIndexList =  dateHourRandomIndexMap.get(date).get(hour)

        // index是在外部进行维护
        var index = 0
        val sessionRandomExtractArray = new ArrayBuffer[SessionRandomExtract]()
        // 开始遍历所有的aggrInfo
        for (sessionAggrInfo <- items) {
          // 如果筛选List中包含当前的index，则提取此sessionAggrInfo中的数据
          if (extractIndexList.contains(index)) {
            val sessionid = StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_SESSION_ID)
            val starttime = StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_START_TIME)
            val searchKeywords = StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS)
            val clickCategoryIds = StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS)
            sessionRandomExtractArray += SessionRandomExtract(taskUUID, sessionid, starttime, searchKeywords, clickCategoryIds)
          }
          // index自增
          index += 1
        }
        sessionRandomExtractArray
    }
    //sessionRandomExtract.count()
    import sparkSession.implicits._
    // 为了方便地将数据保存到MySQL数据库，将RDD数据转换为Dataframe
    sessionRandomExtract.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "session_random_extract")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()
  }


  //各范围session占比统计
  def getSessionRatio(sparkSession: SparkSession, taskUUID: String, value: mutable.HashMap[String, Int]): Unit = {
    val session_count =value.getOrElse(Constants.SESSION_COUNT,1).toDouble
    val visit_length_1s_3s = value.getOrElse(Constants.TIME_PERIOD_1s_3s, 0)
    val visit_length_4s_6s = value.getOrElse(Constants.TIME_PERIOD_4s_6s, 0)
    val visit_length_7s_9s = value.getOrElse(Constants.TIME_PERIOD_7s_9s, 0)
    val visit_length_10s_30s = value.getOrElse(Constants.TIME_PERIOD_10s_30s, 0)
    val visit_length_30s_60s = value.getOrElse(Constants.TIME_PERIOD_30s_60s, 0)
    val visit_length_1m_3m = value.getOrElse(Constants.TIME_PERIOD_1m_3m, 0)
    val visit_length_3m_10m = value.getOrElse(Constants.TIME_PERIOD_3m_10m, 0)
    val visit_length_10m_30m = value.getOrElse(Constants.TIME_PERIOD_10m_30m, 0)
    val visit_length_30m = value.getOrElse(Constants.TIME_PERIOD_30m, 0)

    val step_length_1_3 = value.getOrElse(Constants.STEP_PERIOD_1_3, 0)
    val step_length_4_6 = value.getOrElse(Constants.STEP_PERIOD_4_6, 0)
    val step_length_7_9 = value.getOrElse(Constants.STEP_PERIOD_7_9, 0)
    val step_length_10_30 = value.getOrElse(Constants.STEP_PERIOD_10_30, 0)
    val step_length_30_60 = value.getOrElse(Constants.STEP_PERIOD_30_60, 0)
    val step_length_60 = value.getOrElse(Constants.STEP_PERIOD_60, 0)

    val visit_length_1s_3s_ratio = NumberUtils.formatDouble(visit_length_1s_3s / session_count, 2)
    val visit_length_4s_6s_ratio = NumberUtils.formatDouble(visit_length_4s_6s / session_count, 2)
    val visit_length_7s_9s_ratio = NumberUtils.formatDouble(visit_length_7s_9s / session_count, 2)
    val visit_length_10s_30s_ratio = NumberUtils.formatDouble(visit_length_10s_30s / session_count, 2)
    val visit_length_30s_60s_ratio = NumberUtils.formatDouble(visit_length_30s_60s / session_count, 2)
    val visit_length_1m_3m_ratio = NumberUtils.formatDouble(visit_length_1m_3m / session_count, 2)
    val visit_length_3m_10m_ratio = NumberUtils.formatDouble(visit_length_3m_10m / session_count, 2)
    val visit_length_10m_30m_ratio = NumberUtils.formatDouble(visit_length_10m_30m / session_count, 2)
    val visit_length_30m_ratio = NumberUtils.formatDouble(visit_length_30m / session_count, 2)

    val step_length_1_3_ratio = NumberUtils.formatDouble(step_length_1_3 / session_count, 2)
    val step_length_4_6_ratio = NumberUtils.formatDouble(step_length_4_6 / session_count, 2)
    val step_length_7_9_ratio = NumberUtils.formatDouble(step_length_7_9 / session_count, 2)
    val step_length_10_30_ratio = NumberUtils.formatDouble(step_length_10_30 / session_count, 2)
    val step_length_30_60_ratio = NumberUtils.formatDouble(step_length_30_60 / session_count, 2)
    val step_length_60_ratio = NumberUtils.formatDouble(step_length_60 / session_count, 2)
    // 将统计结果封装为Domain对象
    val sessionAggrStat=SessionAggrStat(taskUUID,
      session_count.toLong, visit_length_1s_3s_ratio, visit_length_4s_6s_ratio, visit_length_7s_9s_ratio,
      visit_length_10s_30s_ratio, visit_length_30s_60s_ratio, visit_length_1m_3m_ratio,
      visit_length_3m_10m_ratio, visit_length_10m_30m_ratio, visit_length_30m_ratio,
      step_length_1_3_ratio, step_length_4_6_ratio, step_length_7_9_ratio,
      step_length_10_30_ratio, step_length_30_60_ratio, step_length_60_ratio)

    import sparkSession.implicits._
    val sessionAggrStatRDD =sparkSession.sparkContext.makeRDD(Array(sessionAggrStat))
    sessionAggrStatRDD.toDF().write.format("jdbc")
      .option("url",ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "session_aggr_stat")//指定写入的表
      .mode(SaveMode.Append)//向表中追加数据，如果不加mode,第二次加入数据时会报错：表格已存在
      .save()
  }



  //过滤session数据
  def filterSessionAndAggrStat(sessionId2FullInfoRDDs: RDD[(String, String)], taskParam: JSONObject, sessionStatAccumulator: SessionStatAccumulator) = {

    // 从taskParam提取限制条件
    val startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE)
    val endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE)
    val professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS)
    val cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES)
    val sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX)
    val searchKeywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS)
    val clickCategories = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS)

    var filterInfo=(if(startAge!=null) Constants.PARAM_START_AGE+"="+startAge+"|" else "")+
      (if(endAge != null) Constants.PARAM_END_AGE + "=" + endAge + "|" else "") +
      (if (professionals != null) Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" else "") +
      (if (cities != null) Constants.PARAM_CITIES + "=" + cities + "|" else "") +
      (if (sex != null) Constants.PARAM_SEX + "=" + sex + "|" else "") +
      (if (searchKeywords != null) Constants.PARAM_KEYWORDS + "=" + searchKeywords + "|" else "") +
      (if (clickCategories != null) Constants.PARAM_CATEGORY_IDS + "=" + clickCategories else "")

    if(filterInfo.endsWith("\\|")){
      filterInfo = filterInfo.substring(0, filterInfo.length  - 1)
    }

    val sessionId2FilteredRDD = sessionId2FullInfoRDDs.filter{
      case(sessionId, fullInfo)=>
        var success=true
        if(!ValidUtils.between(fullInfo, Constants.FIELD_AGE, filterInfo, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)){
          success = false
        }
        if(!ValidUtils.in(fullInfo, Constants.FIELD_PROFESSIONAL, filterInfo, Constants.PARAM_PROFESSIONALS)){
          success = false
        }

        if(!ValidUtils.in(fullInfo, Constants.FIELD_CITY, filterInfo, Constants.PARAM_CITIES)){
          success = false
        }

        if(!ValidUtils.equal(fullInfo, Constants.FIELD_SEX, filterInfo, Constants.PARAM_SEX)){
          success = false
        }

        if (!ValidUtils.in(fullInfo, Constants.FIELD_SEARCH_KEYWORDS, filterInfo, Constants.PARAM_KEYWORDS)){
          success = false
        }
        if (!ValidUtils.in(fullInfo, Constants.FIELD_CATEGORY_ID, filterInfo, Constants.PARAM_CATEGORY_IDS)){
          success = false
        }

        if (success){
          //累加所有符合条件的数据
          sessionStatAccumulator.add(Constants.SESSION_COUNT)
          val stepLength=StringUtils.getFieldFromConcatString(fullInfo,"\\|",Constants.FIELD_STEP_LENGTH).toLong
          val visitLength= StringUtils.getFieldFromConcatString(fullInfo,"\\|",Constants.FIELD_VISIT_LENGTH).toLong

          calculateStepLength(stepLength)
          calculateVisitLength(visitLength)

          def calculateVisitLength(visitLength:Long) ={
            if (visitLength >= 1 && visitLength <= 3) {
              sessionStatAccumulator.add(Constants.TIME_PERIOD_1s_3s)
            } else if (visitLength >= 4 && visitLength <= 6) {
              sessionStatAccumulator.add(Constants.TIME_PERIOD_4s_6s)
            } else if (visitLength >= 7 && visitLength <= 9) {
              sessionStatAccumulator.add(Constants.TIME_PERIOD_7s_9s)
            } else if (visitLength >= 10 && visitLength <= 30) {
              sessionStatAccumulator.add(Constants.TIME_PERIOD_10s_30s)
            } else if (visitLength > 30 && visitLength <= 60) {
              sessionStatAccumulator.add(Constants.TIME_PERIOD_30s_60s)
            } else if (visitLength > 60 && visitLength <= 180) {
              sessionStatAccumulator.add(Constants.TIME_PERIOD_1m_3m)
            } else if (visitLength > 180 && visitLength <= 600) {
              sessionStatAccumulator.add(Constants.TIME_PERIOD_3m_10m)
            } else if (visitLength > 600 && visitLength <= 1800) {
              sessionStatAccumulator.add(Constants.TIME_PERIOD_10m_30m)
            } else if (visitLength > 1800) {
              sessionStatAccumulator.add(Constants.TIME_PERIOD_30m)
            }
          }
          def calculateStepLength(stepLength:Long) ={
            if (stepLength >= 1 && stepLength <= 3) {
              sessionStatAccumulator.add(Constants.STEP_PERIOD_1_3)
            } else if (stepLength >= 4 && stepLength <= 6) {
              sessionStatAccumulator.add(Constants.STEP_PERIOD_4_6)
            } else if (stepLength >= 7 && stepLength <= 9) {
              sessionStatAccumulator.add(Constants.STEP_PERIOD_7_9)
            } else if (stepLength >= 10 && stepLength <= 30) {
              sessionStatAccumulator.add(Constants.STEP_PERIOD_10_30)
            } else if (stepLength > 30 && stepLength <= 60) {
              sessionStatAccumulator.add(Constants.STEP_PERIOD_30_60)
            } else if (stepLength > 60) {
              sessionStatAccumulator.add(Constants.STEP_PERIOD_60)
            }
          }
        }
        success
    }
    sessionId2FilteredRDD
  }



  def getSessionFullInfo(sparkSession: SparkSession, sessionId2GroupRDD: RDD[(String, Iterable[UserVisitAction])]) = {
    val userId2AggrInfoRDD = sessionId2GroupRDD.map{
      case(sessionId,iterableAction)=>
        var startTime:Date=null
        var endTime:Date=null
        val searchKeywords = new StringBuffer("")
        val clickCategries = new StringBuffer("")
        var userId = -1L
        var stepLength = 0L
        for (action<-iterableAction){
          if(userId == -1L){
            userId = action.user_id
          }
          // 更新起始时间和结束时间
          val actionTime = DateUtils.parseTime(action.action_time)
          if(startTime == null || startTime.after(actionTime))
            startTime = actionTime
          if(endTime == null || endTime.before(actionTime))
            endTime = actionTime

          // 完成搜索关键词的追加（去重）
          val searkKeyWord =action.search_keyword
          if(StringUtils.isNotEmpty(searkKeyWord) && !searchKeywords.toString.contains(searkKeyWord))
            searchKeywords.append(searkKeyWord + ",")

          // 完成点击品类的追加（去重）
          val clickCategory = action.click_category_id
          if(clickCategory != -1L && !clickCategries.toString.contains(clickCategory))
            clickCategries.append(clickCategory + ",")
          stepLength += 1
        }
        //去字符串前后逗号
        val searchKW = StringUtils.trimComma(searchKeywords.toString)
        val clickCG = StringUtils.trimComma(clickCategries.toString)
        // 获取访问时长(秒)
        val visitLength =(endTime.getTime-startTime.getTime)/1000
        // 字段名=字段值|字段名=字段值|
        val aggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|" +
          Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKW + "|" +
          Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCG + "|" +
          Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
          Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
          Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime)

        (userId, aggrInfo)
    }
    val sql = "select * from user_info"
    import sparkSession.implicits._
    val userId2UserInfoRDD = sparkSession.sql(sql).as[UserInfo].rdd.map(item => (item.user_id, item))
    //userId2AggrInfoRDD.join(userId2UserInfoRDD) KEY相同的打印出来，不相同的不打印，key是userId
   val sessionId2FullInfoRDD= userId2AggrInfoRDD.join(userId2UserInfoRDD).map{
     //元组匹配
      case(userId,(aggrInfo, userInfo))=>
        val fullInfo = aggrInfo + "|" + Constants.FIELD_AGE + "=" + userInfo.age + "|" +
          Constants.FIELD_PROFESSIONAL + "=" + userInfo.professional + "|" +
          Constants.FIELD_SEX + "=" + userInfo.sex + "|" +
          Constants.FIELD_CITY + "=" + userInfo.city
        // 字段名=字段值|字段名=字段值|。。
        //分隔符如* ^ ： |  .     split的时候需要加上两个斜杠【\\】
        val sessionId = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_SESSION_ID)

        (sessionId, fullInfo)

    }
    sessionId2FullInfoRDD

  }


  def getBasicActionData(sparkSession: SparkSession, taskParam: JSONObject) = {
     val startDate= ParamUtils.getParam(taskParam,Constants.PARAM_START_DATE)
     val endDate=ParamUtils.getParam(taskParam,Constants.PARAM_END_DATE)
     val sql="SELECT * FROM user_visit_action WHERE date>='" + startDate + "' and date<='" + endDate + "'"
    import sparkSession.implicits._
    //as[UserVisitAction]将查询结果封装成样例类 .rdd是DF或DS转换为一个rdd
    sparkSession.sql(sql).as[UserVisitAction].rdd
  }

}

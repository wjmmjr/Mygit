import java.util.UUID

import commons.conf.ConfigurationManager
import commons.constant._
import commons.model.{PageSplitConvertRate, UserVisitAction}
import commons.utils.{DateUtils, ParamUtils}
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable





/**
  * Created by X.Y on 2018/6/27
  */
object PageStatistic {


  def main(args: Array[String]): Unit = {


    val jsonStr=ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskParam=JSONObject.fromObject(jsonStr)

    val sparkConf= new SparkConf().setMaster("local[*]").setAppName("PageStatistic")
    val sparkSession=SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    //生成主键
    val taskUUID =UUID.randomUUID().toString

    //需求五：页面单跳转化率统计
    // 获取配置文件中的pageFlow
    val pageFlow=taskParam.get(Constants.PARAM_TARGET_PAGE_FLOW).toString
    val pageFlowArray =pageFlow.split(",")
    //拉链（1 2 3 4 5 6）与(2 3 4 5 6 7)
    val targetPageSplit =pageFlowArray.slice(0,pageFlowArray.size-1).zip(pageFlowArray.tail).map{
      case(page1,page2)=>(page1+"_"+page2)
    }
    //过滤原始行为数据，以sessionId为K,action为V
    val sessionId2ActionRDD = getSessionAction(sparkSession,taskParam)
    //对相同sessionId数据进行分组，并按时间顺序排序，获得页面访问顺序,过滤获得各页面跳转的集合
    val sessionid2GroupRDD=sessionId2ActionRDD.groupByKey()
    val pageSplitRDD = sessionid2GroupRDD.flatMap{
        case(sesionId,iterableAction)=>
        val sortedAction =iterableAction.toList.sortWith{
          (iterm1,iterm2)=>
            DateUtils.parseTime(iterm1.action_time).getTime < DateUtils.parseTime(iterm2.action_time).getTime//升序排列
        }
          val pageFlow =sortedAction.map(iterm=>iterm.page_id)
          val pageSplit = pageFlow.slice(0,pageFlow.size-1).zip(pageFlow.tail).map{
            case(page1,page2)=>(page1+"_"+page2)
          }
          val pageFilteredSplit =pageSplit.filter{
            iterm=>targetPageSplit.contains(iterm)
          }.map{
            iterm=>(iterm,1L)
          }
          pageFilteredSplit
    }

    //各页面跳转数量统计,针对(K,V)类型的RDD，返回一个(k1->v1,k2->v2....)的map
    val pageCountMap = pageSplitRDD.countByKey()
    val startPageId=pageFlowArray(0).toLong
    val startPageCount =sessionId2ActionRDD.filter{
      case(sessionId,action)=>action.page_id.toLong==startPageId
    }.count()

    // pageConvertMap：用来保存每一个切片对应的转化率
    val pageConvertMap = new mutable.HashMap[String, Double]()

    var lastCount = startPageCount.toDouble
    for(pageSplit <- targetPageSplit){
      val rate =pageCountMap(pageSplit)/lastCount
      pageConvertMap +=(pageSplit->rate)
      lastCount=pageCountMap(pageSplit)
    }
//mkString方法把一个集合转化为一个字符串
    val rateStr = pageConvertMap.map{
      case(item,rate)=>item+"="+rate
    }.mkString("|")

    val pageSplit =  PageSplitConvertRate(taskUUID,rateStr)
    val pageSplitNewRDD = sparkSession.sparkContext.makeRDD(Array(pageSplit))
    import sparkSession.implicits._
    pageSplitNewRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "page_split_convert_rate0115")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()

  }



  def getSessionAction(sparkSession: SparkSession, taskParam: JSONObject) ={
    val startDate =taskParam.get(Constants.PARAM_START_DATE).toString
    val endDate=taskParam.get(Constants.PARAM_END_DATE).toString
    val sql="select * from user_visit_action where date>='" + startDate + "' and date<='" + endDate + "'"
    import sparkSession.implicits._
    sparkSession.sql(sql).as[UserVisitAction].rdd.map{
      iterm=>(iterm.session_id,iterm)
    }
  }

}

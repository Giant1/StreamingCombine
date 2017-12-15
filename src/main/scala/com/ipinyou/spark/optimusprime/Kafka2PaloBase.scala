package com.ipinyou.spark.optimusprime


import java.text.SimpleDateFormat
import java.util.Date

import com.standstorm.table.RptEffectBase
import com.typesafe.config.ConfigFactory
import kafka.serializer.StringDecoder
import org.apache.hadoop.io.NullWritable
import org.slf4j.LoggerFactory

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext

import org.apache.spark.sql.types._
import scala.sys.process._

import org.apache.spark.streaming.dstream.DStream

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.{OffsetRange, HasOffsetRanges}
import scala.collection.mutable.ListBuffer


import org.apache.spark.streaming.kafka.KafkaManager

/**
  * Created by giant on 17/8/19.
  */

class Kafka2PaloBase {

}

object Kafka2PaloBase {
  final val SEPERATOR = "\t"
  final val DALAY_TIMES = 2 * 60 * 60 * 1000
  val logger = LoggerFactory.getLogger(CombinerHourly2.getClass)
  System.setProperty("hadoop.home.dir", "D:\\hadoop\\hadoop2")

  //System.setProperty("HADOOP_CONF_DIR", "D:\\hadoop\\hadoop2\\etc\\hadoop")

  def nilToZero(_value: String): String = {
    val value = if (_value == "null" || _value.isEmpty) "0" else _value
    value
  }


  def parseMessage(message: String): RptEffectBaseTable = {
    val baseTable = new RptEffectBaseTable()
    val array = message.split(CombinerHourly2.SEPERATOR)

    baseTable.partner_id = nilToZero(array(1)).toLong
    baseTable.advertiser_company_id = nilToZero(array(2)).toLong
    baseTable.advertiser_id = nilToZero(array(3)).toLong
    baseTable.order_id = nilToZero(array(4)).toLong
    baseTable.campaign_id = nilToZero(array(5)).toLong
    baseTable.sub_campaign_id = nilToZero(array(6)).toLong
    baseTable.exe_campaign_id = nilToZero(array(7)).toLong
    baseTable.vertical_tag_id = nilToZero(array(8)).toLong
    baseTable.conversion_pixel = nilToZero(array(9)).toLong
    baseTable.creative_size = array(10)
    baseTable.creative_id = array(11)
    baseTable.creative_type = array(12)
    baseTable.country_id = nilToZero(array(13)).toLong
    baseTable.province_id = nilToZero(array(14)).toLong
    baseTable.city_id = nilToZero(array(15)).toLong
    baseTable.inventory_type = array(16)
    baseTable.ad_slot_type = array(17)
    baseTable.banner_view_type = array(18)
    baseTable.video_view_type = array(19)
    baseTable.native_view_type = array(20)
    baseTable.ad_unit_id = array(21)
    baseTable.ad_unit_width = nilToZero(array(22)).toLong
    baseTable.ad_unit_height = nilToZero(array(23)).toLong
    baseTable.platform = array(24)
    baseTable.domain_category = array(25)
    baseTable.top_level_domain = array(26)
    baseTable.domain = array(27)
    baseTable.app_category = array(28)
    baseTable.app_name = array(29)
    baseTable.app_id = array(30)
    baseTable.deal_type = array(33)
    baseTable.deal_id = array(34)
    baseTable.device_type = array(35)
    baseTable.os = array(36)
    baseTable.browser = array(37)
    baseTable.brand = array(38)
    baseTable.model = array(39)
    baseTable.network_generation = array(40)
    baseTable.carrier = array(41)
    baseTable.mob_device_type = array(42)
//    baseTable.mac = array(43)
//    baseTable.mac_enc = array(44)
//    baseTable.imei = array(45)
//    baseTable.imei_enc = array(46)
//    baseTable.imsi = array(47)
//    baseTable.imsi_enc = array(48)
//    baseTable.dpid = array(49)
//    baseTable.dpidenc = array(50)
//    baseTable.adid = array(51)
//    baseTable.adid_enc = array(52)

    //hour表630增加字段
    if(array.length>=103) {
      baseTable.client_id = nilToZero(array(97)).toLong
      baseTable.campaign_division_id = nilToZero(array(98)).toLong
      baseTable.sub_campaign_division_id = nilToZero(array(99)).toLong
      baseTable.exe_campaign_division_id = nilToZero(array(100)).toLong
      baseTable.sub_platform = array(101)
    }

    //metrics
    baseTable.raw_media_cost = nilToZero(array(53)).toDouble
    baseTable.media_cost = nilToZero(array(54)).toDouble
    baseTable.service_fee = nilToZero(array(55)).toDouble
    baseTable.media_tax = nilToZero(array(56)).toDouble
    baseTable.service_tax = nilToZero(array(57)).toDouble
    baseTable.total_cost = nilToZero(array(58)).toDouble
    baseTable.system_loss = nilToZero(array(59)).toDouble
    baseTable.bid = nilToZero(array(61)).toLong
    baseTable.imp = nilToZero(array(62)).toLong
    baseTable.click = nilToZero(array(63)).toLong
    baseTable.reach = nilToZero(array(64)).toLong
    baseTable.two_jump = nilToZero(array(65)).toLong
    baseTable.click_conversion = nilToZero(array(66)).toLong
    baseTable.imp_conversion = nilToZero(array(67)).toLong

    baseTable
  }

  def dayTime(line: String): (String, String) = {

    val array = line.split(CombinerHourly2.SEPERATOR)
    val requestTime = array(0)
    //println(requestTime)
    val day = requestTime.substring(0, 8)
    val hour = requestTime.substring(8, 10)
    (day, hour)
  }

  import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat

  class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {
    override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String =
      key.asInstanceOf[String] + "-" + name

    override def generateActualKey(key: Any, value: Any): Any =
      NullWritable.get()
  }

  implicit class RDDOps[K, V](rdd: RDD[(K, V)]) {
    def partitionBy(func: (K, V) => Boolean): (RDD[(K, V)], RDD[(K, V)]) = {
      //val passes = rdd.filter((x,y)=> func(x,y))
      //val fails = rdd.filter((x,y) => !func(x,y))
      val passes = rdd.filter(x => func(x._1, x._2))
      val fails = rdd.filter(x => !func(x._1, x._2))
      (passes, fails)
    }
  }

  implicit class DStreamOps[K, V](rdd: DStream[(K, V)]) {
    def partitionBy(func: (K, V) => Boolean): (DStream[(K, V)], DStream[(K, V)]) = {
      //val passes = rdd.filter((x,y)=> func(x,y))
      //val fails = rdd.filter((x,y) => !func(x,y))
      val passes = rdd.filter(x => func(x._1, x._2))
      val fails = rdd.filter(x => !func(x._1, x._2))
      (passes, fails)
    }
  }

  def isValidData(line: String): Boolean = {
    if (line.split(SEPERATOR).length < 96 || line.indexOf("java.lang.String") > 0)
      false
    else
      true
  }

  /**
    *
    * @param rdd
    * @return
    */
  def aggregation(rdd: RDD[String]): RDD[String] = {
    val reduceRDD = rdd
      .map { line =>
        val (requestDay, hour) = dayTime(line)
        val base: RptEffectBaseTable = parseMessage(line)
        base.pday = requestDay.toLong
        base.phour = hour.toLong
        val daytime = requestDay + hour

        ((base.dimToString, daytime),
          (base.raw_media_cost, base.media_cost, base.service_fee, base.media_tax, base.service_tax,
            base.total_cost, base.system_loss, base.bid, base.imp, base.click, base.reach, base.two_jump,
            base.click_conversion, base.imp_conversion
            )
          )
      }.reduceByKey { (x1, x2) =>
      (x1._1 + x2._1, x1._2 + x2._2, x1._3 + x2._3, x1._4 + x2._4, x1._5 + x2._5, x1._6 + x2._6, x1._7 + x2._7,
        x1._8 + x2._8, x1._9 + x2._9, x1._10 + x2._10, x1._11 + x2._11, x1._12 + x2._12, x1._13 + x2._13, x1._14 + x2._14)
    }.map { x =>
      val (dimension, daytime) = (x._1._1, x._1._2)
      val (raw_media_cost, media_cost, serviceFee, mediaTax, serviceTax, totalCost, systemLoss, bid, imp, click, reach, two_jump, clickConversion, impConversion) =
        (x._2._1, x._2._2, x._2._3, x._2._4, x._2._5, x._2._6, x._2._7, x._2._8, x._2._9, x._2._10, x._2._11, x._2._12, x._2._13, x._2._14)

//      val f_sub_platform =  if (sub_platform.isEmpty || sub_platform == "") "null" else sub_platform
      val result =s"""$dimension\t$raw_media_cost\t$media_cost\t$serviceFee\t$mediaTax\t$serviceTax\t$totalCost\t$systemLoss\t$bid\t$imp\t$click\t$reach\t$two_jump\t$clickConversion\t$impConversion""".stripMargin

      result
    }
    reduceRDD
  }



  //val file = new java.io.File("conf/application.conf")
  //val config = ConfigFactory.load//ConfigFactory.parseFile(new java.io.File("conf/application.conf"))
  val config = ConfigFactory.parseFile(new java.io.File("conf/kafka2palobase.conf"))


  object SQLHiveContextSingleton {
    @transient private var instance: HiveContext = _

    def getInstance(sparkContext: SparkContext): HiveContext = {
      synchronized {
        if (instance == null) {
          instance = new HiveContext(sparkContext)
        }
        instance
      }
    }
  }

  def writeDataByTable(stream: DStream[(String, String)],
                       table: String,
                       schema: StructType,
                       sqlContext: HiveContext, km: KafkaManager): Unit = {
    var offsetRanges = Array[OffsetRange]()
    val data = stream
      .map { event =>

        val (rday, rhour) = {
          (event._1.substring(0, 8), event._1.substring(8))
        }
        Row.merge(Row.fromSeq(event._2.split("\t")), Row.fromSeq(Seq(rday, rhour)))
      }
    val numberOfFilesPerBatch = config.getInt("spark.numFilesPerBatch")

    data.repartition(numberOfFilesPerBatch)
      .foreachRDD { (rdd, time) =>
        val df = sqlContext.createDataFrame(rdd, schema)

        df.write.format("orc").mode(SaveMode.Append).partitionBy("pday", "phour")
          .saveAsTable(table)

        km.updateZKOffsetsWithRow(rdd)
        for (o <- offsetRanges) {
          logger.info(s"${o.topic} .... partition->${o.partition} .... fromOffset->${o.fromOffset} .... untilOffset->${o.untilOffset}")
        }
      }
  }


  def initializeContext(): Any = {
    val sparkConf = new SparkConf().setAppName(config.getString("spark.app_name"))
      .setMaster(config.getString("spark.mode"))
      .set("spark.yarn.queue", config.getString("spark.queue"))
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(config.getInt("spark.duration")))
    (sc, ssc)
  }


  def processRDD(rdd: RDD[(String, String, String)])(implicit sqlContext: HiveContext): Unit = {
    val message = rdd.map(_._2)

    val aggregationRDD = aggregation(message)

    val numberOfFilesPerBatch = config.getInt("spark.numFilesPerBatch")
//    val data = rdd.repartition(numberOfFilesPerBatch)
    val data = aggregationRDD.filter(line => {!line.equals("\n")})

    val date = new Date();
    val formatter = new SimpleDateFormat ("yyyyMMddHHmm")
    val timestr = formatter.format(date)
    val path = config.getString("spark.result_path") + timestr

    val commands = "hadoop fs -rmr " + path + ""!

    data.saveAsTextFile(path)

    new PaloOperator().execLoadBase(timestr)

  }

  /**
    * Main
    *
    * @param args
    */
  def main(args: Array[String]) {

    //设置打印日志级别
    //        Logger.getLogger("org.apache.kafka").setLevel(Level.ERROR)
    //        Logger.getLogger("org.apache.zookeeper").setLevel(Level.ERROR)
    //        Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    logger.info(" -- Start -- ")


    val (sc: SparkContext, ssc: StreamingContext) = initializeContext

    // Create direct kafka stream with brokers and topics
    val topics = config.getString("kafka.topics").split(",").toSet
    //val topicsSet = topics.split(",").toSet
    val (broker, groupid) = (config.getString("kafka.brokers"), config.getString("kafka.groupid"))
    logger.info(s"broker--->$broker \n  topics--->$topics, --groupid--->$groupid")

    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> broker,
      "group.id" -> groupid,
      "auto.offset.reset" -> config.getString("kafka.offset_reset") // Wrong value latest of auto.offset.reset in ConsumerConfig; Valid values are smallest and largest earliest
    )

    val km = new KafkaManager(kafkaParams)
    val stream = km.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,
      kafkaParams, topics)

    var offsetRanges = Array[OffsetRange]()


    implicit val sqlContext = new HiveContext(sc)
    sqlContext.setConf("hive.exec.dynamic.partition", "true")
    sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    sqlContext.setConf("hive.exec.stagingdir", config.getString("spark.stagingdir"))
    val message = stream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      rdd
    }.foreachRDD { rdd =>
      // foreachRDD是DStream的output操作
      processRDD(rdd)

      for (o <- offsetRanges) {
        logger.info(s"${o.topic} .... partition->${o.partition} .... fromOffset->${o.fromOffset} .... untilOffset->${o.untilOffset}")
      }

      km.updateZKOffsets(rdd)
    }

    ssc.start()
    ssc.awaitTermination()
  }
}


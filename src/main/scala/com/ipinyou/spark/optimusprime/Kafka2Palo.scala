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


import org.apache.spark.streaming.kafka.KafkaManager

/**
  * Created by giant on 17/8/19.
  */

class Kafka2Palo {

}

object Kafka2Palo {
  final val SEPERATOR = "\t"
  final val DALAY_TIMES = 2 * 60 * 60 * 1000
  val logger = LoggerFactory.getLogger(CombinerHourly2.getClass)
  System.setProperty("hadoop.home.dir", "D:\\hadoop\\hadoop2")

  //System.setProperty("HADOOP_CONF_DIR", "D:\\hadoop\\hadoop2\\etc\\hadoop")

  def nilToZero(_value: String): String = {
    val value = if (_value == "null" || _value.isEmpty) "0" else _value
    value
  }

  def parseMessage(message: String): RptEffectBase = {
    val base = new RptEffectBase()
    val array = message.split(CombinerHourly2.SEPERATOR)

    base.setPartnerId(nilToZero(array(1)).toLong)
    base.setAdvertiserCompanyId(nilToZero(array(2)).toLong)
    base.setAdvertiserId(nilToZero(array(3)).toLong)
    base.setOrderId(nilToZero(array(4)).toLong)
    base.setCampaignId(nilToZero(array(5)).toLong)
    base.setSubCampaignId(nilToZero(array(6)).toLong)
    base.setExeCampaignId(nilToZero(array(7)).toLong)
    base.setVerticalTagId(nilToZero(array(8)).toLong)
    base.setConversionPixel(nilToZero(array(9)).toLong)
    base.setCreativeSize(array(10))
    base.setCreativeId(array(11))
    base.setCreativeType(array(12))
    base.setInventoryType(array(16))
    base.setAdSlotType(array(17))
    base.setPlatform(array(24))

    //hour表630增加字段
    if(array.length>=103) {
      base.setClientId(nilToZero(array(97)).toLong)
      base.setCampaignDivisionId(nilToZero(array(98)).toLong)
      base.setSubCampaignDivisionId(nilToZero(array(99)).toLong)
      base.setExeCampaignDivisionId(nilToZero(array(100)).toLong)
      base.setSubPlatform(array(101))
      base.setRequestDay(array(102).toLong)
      base.setRequestHour(array(103).toLong)
    }

    //number
    base.setRaw_media_cost(nilToZero(array(53)).toDouble)
    base.setMedia_cost(nilToZero(array(54)).toDouble)
    base.setServiceFee(nilToZero(array(55)).toDouble)
    base.setMediaTax(nilToZero(array(56)).toDouble)
    base.setServiceTax(nilToZero(array(57)).toDouble)
    base.setTotalCost(nilToZero(array(58)).toDouble)
    base.setSystemLoss(nilToZero(array(59)).toDouble)
    base.setBid(nilToZero(array(61)).toLong)
    base.setImp(nilToZero(array(62)).toLong)
    base.setClick(nilToZero(array(63)).toLong)
    base.setReach(nilToZero(array(64)).toLong)
    base.setTwo_jump(nilToZero(array(65)).toLong)
    base.setClickConversion(nilToZero(array(66)).toLong)
    base.setImpConversion(nilToZero(array(67)).toLong)
    base.setDay(nilToZero(array(81)).toInt)

    base
  }

  def main5(args: Array[String]) {
    val messages = scala.collection.mutable.ListBuffer[String]()
    messages += """20170426155102486	19	4187	50027	170	9900	null	9905	15	null	140x425	creative/native/2963	nativ	1156000000	1156330000	1156330600	pc	banner	fixed			9050601115763107	140	425	gdt	unknown		null								pc	na	na																null	null	null	null	null	null	null	null	1	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	20170426	15	3	default_cpm#general	null	rank#defaultset	null	null	null	null	null	L024b21P055b0E7TnAP-			M,edu_coolege,age_19_24,consuming_prower_low	gdt$M,gdt$edu_coolege,gdt$age_19_24,gdt$consuming_prower_low"""
    messages += """20170426155102040	19	4187	50027	170	9900	null	9902	15	null	140x425	creative/native/2963	nativ	1156000000	1156440000	1156440100	pc	banner	fixed			9050601115763107	140	425	gdt	unknown		null								pc	na	na																null	null	null	null	null	null	null	null	1	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	20170426	15	3	default_cpm#general	null	rank#defaultset	null	null	null	null	null	L024b21P055b0E7TJHOW			M,edu_coolege,age_0_18,consuming_prower_high	gdt$M,gdt$edu_coolege,gdt$age_0_18,gdt$consuming_prower_high"""
    messages += """20170426155102751	19	4187	50027	170	9900	null	9902	15	null	140x425	creative/native/2963	nativ	1156000000	1156350000	1156350300	pc	banner	fixed			9050601115763107	140	425	gdt	unknown		null								pc	na	na																null	null	null	null	null	null	null	null	1	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	20170426	15	3	default_cpm#general	null	rank#defaultset	null	null	null	null	null	L024b21P055b0E7U3YmP			M,age_25_34,consuming_prower_high	gdt$M,gdt$age_25_34,gdt$consuming_prower_high"""
    messages += """20170426155100496	19	4187	50027	170	9900	null	9902	15	null	140x425	creative/native/2962	nativ	1156000000	1156440000	1156440100	pc	banner	fixed			9050601115763107	140	425	gdt	unknown		null								pc	na	na																null	null	null	null	null	null	null	null	1	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	20170426	15	3	default_cpm#general	null	rank#defaultset	null	null	null	null	null	L024b21P055-0E7R-19D			M,edu_coolege,age_35_49	gdt$M,gdt$edu_coolege,gdt$age_35_49"""
    messages += """20170426155101172	19	4187	50027	170	9900	null	9905	15	null	140x425	creative/native/2962	nativ	1156000000	1156510000	1156510100	pc	banner	fixed			9050601115763107	140	425	gdt	unknown		null								pc	na	na																null	null	null	null	null	null	null	null	1	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	20170426	15	3	default_cpm#general	null	rank#defaultset	null	null	null	null	null	L024b21P055a0E7SLZv-			M,edu_coolege,age_35_49,consuming_prower_high	gdt$M,gdt$edu_coolege,gdt$age_35_49,gdt$consuming_prower_high"""
    messages += """20170426155102675	19	4187	50027	170	9900	null	9902	15	null	140x425	creative/native/2963	nativ	1156000000	1156370000	1156371400	pc	banner	fixed			9050601115763107	140	425	gdt	unknown		null								pc	na	na																null	null	null	null	null	null	null	null	1	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	20170426	15	3	default_cpm#general	null	rank#defaultset	null	null	null	null	null	L024b21P055b0E7T_F6T			F,edu_high_school,age_35_49	gdt$F,gdt$edu_high_school,gdt$age_35_49"""
    messages += """20170426155102643	19	4187	50027	170	9900	null	9902	15	null	140x425	creative/native/2962	nativ	1156000000	1156130000	1156131000	pc	banner	fixed			9050601115763107	140	425	gdt	unknown		null								pc	na	na																null	null	null	null	null	null	null	null	1	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	20170426	15	3	default_cpm#general	null	rank#defaultset	null	null	null	null	null	L024b21P055b0E7Tx4-z			M,edu_coolege,age_25_34,consuming_prower_low	gdt$M,gdt$edu_coolege,gdt$age_25_34,gdt$consuming_prower_low"""
    messages += """20170426155101201	19	4187	50027	170	9900	null	9902	15	null	140x425	creative/native/2962	nativ	1156000000	1156330000	1156330700	pc	banner	fixed			9050601115763107	140	425	gdt	unknown		null								pc	na	na																null	null	null	null	null	null	null	null	1	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	20170426	15	3	default_cpm#general	null	rank#defaultset	null	null	null	null	null	L024b21P055a0E7SNONr			F,edu_coolege,age_19_24,consuming_prower_low	gdt$F,gdt$edu_coolege,gdt$age_19_24,gdt$consuming_prower_low"""
    messages += """20170426155102481	19	4187	50027	170	9900	null	9902	15	null	140x425	creative/native/2963	nativ	1156000000	1156350000	1156350100	pc	banner	fixed			9050601115763107	140	425	gdt	unknown		null								pc	na	na																null	null	null	null	null	null	null	null	1	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	20170426	15	3	default_cpm#general	null	rank#defaultset	null	null	null	null	null	L024b21P055b0E7TmKoR			M,edu_coolege,age_25_34,consuming_prower_low	gdt$M,gdt$edu_coolege,gdt$age_25_34,gdt$consuming_prower_low"""
    messages += """20170426155100533	19	4187	50027	170	9900	null	9905	15	null	140x425	creative/native/2962	nativ	1156000000	1156500000	1156500000	pc	banner	fixed			9050601115763107	140	425	gdt	unknown		null								pc	na	na																null	null	null	null	null	null	null	null	1	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	20170426	15	3	default_cpm#general	null	rank#defaultset	null	null	null	null	null	L010l_1P055-0YhsnNne			F,edu_primary_school,age_35_49,consuming_prower_high	gdt$F,gdt$edu_primary_school,gdt$age_35_49,gdt$consuming_prower_high"""
    messages.foreach { mess =>
      println(parseMessage(mess))
    }

    messages.map { line =>
      val base: RptEffectBase = parseMessage(line)
      ((base.getPartnerId), (base.getRaw_media_cost))
    }

  }

  def main_debug_conv_imp(args: Array[String]) {
    val messages = scala.collection.mutable.ListBuffer[String]()
    messages +="""20170502093843915	[Ljava.lang.String;@160997f6	null	null	null	null	null	null	null	null	null	null	null	null	null	null	1	null	null	null	null	null	null	null	null	null	null	null	null	null	20170502	09	0	default_cpaorder#ctvRandom	null	rank#defaultset	null	null	null	-1	-1	L038S41Oz44w0ngpfVNU	G8PEgOBmz9G	8:8621#0xg0e0xe0t0wt290wq110wp140wo0y0wn160wm170wi0c0w10q=37400.0,8:8621:c#0xg040xe090wt0o0wq010wp070wo0b0wn0d0wm080wi040w106=8700.0,8:8621:p#0xg010xe050wt0k0wp030wo060wn0a0wm030wi010w1020vi01=5300.0,90008621=4960.0,8:8621:cp_9846#0xg010xe050wt0h0wp030wo040wn090wm030wi010w1020ur01=4500.0,64027=4200.0,64041=3100.0,64149=2300.0,64031=2200.0,64030=2000.0,64029=1800.0,64033=1700.0,64028=1700.0,8:8621:cp_9844#0xg010xe010wt020wp010wo030wn010wm010wi010w101=1200.0,8:8621:cp_9842#0xg010xe010wt010wq010wp020wo010wn010wm010wi010w102=1200.0,8:8621:cp_9845#0xg010xe010wt020wp010wo010wn010wm020wi010w101=1100.0,nrtc:cluster_7193:cvr|0xg003I=1000.2309,nrtc:cluster_8346:cvr|0xg001K=1000.1082,nrtc:cluster_1948:cvr|0xg0016=1000.0688,rtc:200000005:cvr|0xg000Y=1000.0604,65316=1000.0,8:8621:cp_9843#0xe010wt020wp010wo020wn010wm01=800.0,8:1631#0xf08=800.0,65381=800.0,64142=700.0,64032=700.0,64654=300.0,64440=300.0,90001631=240.0,8:1631:p#0xf02=200.0,baidu$99=100.0,baidu$927=100.0,baidu$92=100.0,baidu$91=100.0,baidu$90=100.0,baidu$89=100.0,baidu$88=100.0,baidu$85=100.0,baidu$83=100.0,baidu$82=100.0,baidu$790=100.0,baidu$76=100.0,baidu$535=100.0,baidu$528=100.0,baidu$522=100.0,baidu$521=100.0,baidu$518=100.0,baidu$474=100.0,baidu$453=100.0,baidu$444=100.0,baidu$436=100.0,baidu$425=100.0,baidu$399=100.0,baidu$397=100.0,baidu$393=100.0,baidu$391=100.0,baidu$385=100.0,baidu$378=100.0,baidu$351=100.0,baidu$315=100.0,baidu$312=100.0,baidu$311=100.0,baidu$309=100.0,baidu$303=100.0,baidu$291=100.0,baidu$282=100.0,baidu$274=100.0,baidu$266=100.0,baidu$263=100.0,baidu$262=100.0,baidu$248=100.0,baidu$247=100.0,baidu$246=100.0,baidu$240=100.0,baidu$239=100.0,baidu$205=100.0,baidu$204=100.0,baidu$202=100.0,baidu$201=100.0,baidu$199=100.0,baidu$195=100.0,baidu$193=100.0,baidu$182=100.0,baidu$169=100.0,baidu$166=100.0,baidu$148=100.0,baidu$147=100.0,baidu$144=100.0,baidu$11=100.0,90003245=100.0,8:1631:l45#0xf01=100.0,72092=100.0,72091=100.0,72085=100.0,72078=100.0,72061=100.0,72057=100.0,72045=100.0,72043=100.0,72035=100.0	null	null"""
    messages +="""20170502132557966	[Ljava.lang.String;@228447ba	null	null	null	null	null	null	null	null	null	null	null	null	null	null	1	null	null	null	null	null	null	null	null	null	null	null	null	null	20170502	13	0	default_cpaorder#ctvOptPriceModel	null	rank#defaultset	null	null	null	-1	-1	L014sV1P1yMq3BzM31xU	G959Of7vce_Y	8:7860#0xp070xe050xb0t0xa060x91v0x817=15700.0,8:955#0xn0f0xb050x9010x80h0x3150x21z=15000.0,click_baidu#24q0jD2=10000.0,8:955:p#0xn070x9010x8070x30u0x20w=7700.0,90007860=4710.0,8:955:s#0xn080x8010x30c0x20p=4600.0,90000955=4504.0,8:7962#0xt0g=1600.0,61104=1100.0,nrtc:cluster_7962:cvr|0xt03yN=1013.69696,nrtc:cluster_7422:cvr|0xt00rl0xp004c=1001.95715,rtc:7962:cvr|0xt00nC=1001.47656,rtc:6203:cvr|0xt00io=1001.1534,8:955:o#0xb020x8040x204=1000.0,8:955:cp_1255#0x8010x3050x204=1000.0,8:955:c#0x8010x3050x204=1000.0,64380=900.0,13866=879.0497,10111=811.8,64149=700.0,64028=600.0,8:7962:p#0xt05=500.0,65381=500.0,65317=500.0,90007962=480.0,8:3531#0xl04=400.0,8:200000124#0xo03=300.0,65386=300.0,64030=300.0,765381=200.0,64029=200.0,16737=192.7372,10005=178.0,16848=149.99677,16851=147.32715,90003531=120.0,16699=114.83909,baidu$950=100.0,baidu$927=100.0,baidu$92=100.0,baidu$91=100.0,baidu$89=100.0,baidu$83=100.0,baidu$80=100.0,baidu$76=100.0,baidu$731=100.0,baidu$729=100.0,baidu$728=100.0,baidu$720=100.0,baidu$717=100.0,baidu$716=100.0,baidu$535=100.0,baidu$502=100.0,baidu$445=100.0,baidu$444=100.0,baidu$441=100.0,baidu$436=100.0,baidu$425=100.0,baidu$397=100.0,baidu$393=100.0,baidu$391=100.0,baidu$386=100.0,baidu$385=100.0,baidu$378=100.0,baidu$377=100.0,baidu$376=100.0,baidu$375=100.0,baidu$373=100.0,baidu$351=100.0,baidu$343=100.0,baidu$325=100.0,baidu$318=100.0,baidu$312=100.0,baidu$303=100.0,baidu$291=100.0,baidu$287=100.0,baidu$282=100.0,baidu$275=100.0,baidu$274=100.0,baidu$266=100.0,baidu$263=100.0,baidu$262=100.0,baidu$251=100.0,baidu$248=100.0,baidu$247=100.0,baidu$245=100.0,baidu$237=100.0,baidu$205=100.0,baidu$204=100.0,baidu$202=100.0,baidu$199=100.0,baidu$193=100.0,baidu$182=100.0,baidu$169=100.0,baidu$147=100.0,baidu$144=100.0,baidu$11=100.0,8:3531:p#0xl01=100.0,8:1371:p#0xe01=100.0,8:1371#0xe01=100.0	null	null17/05/06 00:20:24 INFO DAGScheduler: Job 1 finished: print at CombinerHourly2.scala:457, took 1.746102 s"""
    messages +="""20170502145647634	[Ljava.lang.String;@71ecd60d	null	null	null	null	null	null	null	null	null	null	null	null	null	null	1	null	null	null	null	null	null	null	null	null	null	null	null	null	20170502	14	0	default_cpm#general	null	rank#defaultset	null	null	null	-1	13	L039Vq1OzEgx04OZv-Xx	96f1e40c627751c9db180ea148287dfe	8:1123:p#0x4030wq070wi04=1400.0,8:1123#0x4030wq070wi04=1400.0,40017=216.0,40014=216.0,90001123=190.0,baidu$378=100.0,baidu$377=100.0,baidu$376=100.0,baidu$375=100.0,765410=100.0,65410=100.0,30009=100.0,30006=100.0,30005=100.0,40092=57.0,40060=54.0,40062=51.0,36021=5.0,36001=5.0,36023=4.0,36026=1.0	null	null"""
    messages +="""20170502211617569	[Ljava.lang.String;@163b9048	null	null	null	null	null	null	null	null	null	null	null	null	null	null	1	null	null	null	null	null	null	null	null	null	null	null	null	null	20170502	21	0	default_cpaorder#ctvRandom	null	rank#defaultset	null	null	null	-1	-1	L012dj1P1oax2yI7WWEZ	GC2HrG3PchWR	8:1765#0xr070xq05=1200.0,rtcc:X400022402:cvr|0xr01Ny=1006.9176,nrtc:cluster_7422:cvr|0xr00Lv=1002.9502,rtc:1765:cvr|0xr00oJ=1001.53796,8:1765:p#0xr040xq05=900.0,90001765=360.0,8:1765:c#0xr03=300.0,8:4328:cp_10333#0xq02=200.0,8:4328:c#0xq02=200.0,8:1765:cp_5585#0xr02=200.0,64149=200.0,64041=200.0,64033=200.0,64032=200.0,baidu$385=100.0,baidu$378=100.0,baidu$202=100.0,baidu$192=100.0,8:1765:cp_3893#0xr01=100.0,64031=100.0,64027=100.0,600793=100.0,90004328=60.0,13677=9.0,13496=9.0,10683=9.0,16753=0.7,10131=0.7,16823=0.076593295	192,202,385,378	baidu$192,baidu$202,baidu$385,baidu$378"""
    messages +="""20170502174033064	[Ljava.lang.String;@142ed7b	null	null	null	null	null	null	null	null	null	null	null	null	null	null	1	null	null	null	null	null	null	null	null	null	null	null	null	null	20170502	17	0	default_cpaorder#ctvRandom	null	rank#defaultset	null	null	null	-1	-1	L004I11P1zPe3EsRNxLS	G1C7Lr4PcgGS	8:1371:p#0xo020xn1d0xm1g0xl0a0xg0h0xe660xd8v0xc6i0xb0s=93300.0,8:1371#0xo010xn0p0xm0q0xl050xg080xe280xd1o0xc6i0xb0s=46700.0,8:1371#0xo010xn0o0xm0q0xl050xg090xe3y0xd77=46600.0,90001371=28080.0,click_ipinyou#24q0jd324q0jb6=10000.0,click_criteo#24q0jv224q0ju224q0jl2=10000.0,64380=3900.0,8:7962#0xl020xk020xb020x3090x103=1800.0,64027=1400.0,nrtc:cluster_7648:cvr|0xt00I8=1002.7378,nrtc:cluster_7422:cvr|0xt00Du=1002.4518,nrtc:cluster_7962:cvr|0xt00AY=1002.29694,nrtc:200000124:cvr|0xt00wI=1002.0358,nrtc:6203:cvr|0xt00rS=1001.73676,nrtc:7979:cvr|0xt00pK=1001.6075,nrtc:cluster_7193:cvr|0xt00lG=1001.3449,rtc:2786:cvr|0xt00hC=1001.0925,nrtc:cluster_1824:cvr|0xt004g=1000.27155,90007962=1000.0,8:7962:p#0xl020xk020xb020x3020x101=900.0,64424=800.0,64149=800.0,64030=700.0,16848=508.03815,16851=506.0293,65316=400.0,8:7962:cp_9075#0xl010xk010xb01=300.0,8:7962:c#0xl010xk010xb01=300.0,8:7486#0xg02=200.0,8:1148:cp_6195#0wv02=200.0,8:1148:c#0wv02=200.0,65311=200.0,10005=118.0,lingji$7030025=100.0,baidu$99=100.0,baidu$92=100.0,baidu$91=100.0,baidu$90=100.0,baidu$89=100.0,baidu$88=100.0,baidu$85=100.0,baidu$84=100.0,baidu$83=100.0,baidu$825=100.0,baidu$82=100.0,baidu$80=100.0,baidu$78=100.0,baidu$748=100.0,baidu$735=100.0,baidu$683=100.0,baidu$535=100.0,baidu$425=100.0,baidu$385=100.0,baidu$308=100.0,baidu$295=100.0,baidu$291=100.0,baidu$287=100.0,baidu$286=100.0,baidu$282=100.0,baidu$274=100.0,baidu$270=100.0,baidu$266=100.0,baidu$263=100.0,baidu$262=100.0,baidu$251=100.0,baidu$248=100.0,baidu$247=100.0,baidu$246=100.0,baidu$245=100.0,baidu$244=100.0,baidu$240=100.0,baidu$239=100.0,baidu$214=100.0,baidu$213=100.0,baidu$205=100.0,baidu$204=100.0,baidu$202=100.0,baidu$201=100.0,baidu$193=100.0,baidu$185=100.0,baidu$182=100.0,baidu$169=100.0,baidu$166=100.0,baidu$148=100.0,baidu$147=100.0,baidu$144=100.0,baidu$143=100.0,baidu$134=100.0,baidu$11=100.0,8:955:p#0xp01=100.0,8:955#0xp01=100.0,765433=100.0,765316=100.0,765311=100.0,72092=100.0,72090=100.0,72085=100.0,72078=100.0,72075=100.0,72073=100.0	83,535,735,825,11,78,89,91,92,148,182,185,214,240,248,263,266,274,291,295	baidu$83,baidu$535,baidu$735,baidu$825,baidu$11,baidu$78,baidu$89,baidu$91,baidu$92,baidu$148,baidu$182,baidu$185,baidu$214,baidu$240,baidu$248,baidu$263,baidu$266,baidu$274,baidu$291,baidu$295"""
    messages +="""20170502224355975	[Ljava.lang.String;@35ad0525	null	null	null	null	null	null	null	null	null	null	null	null	null	null	1	null	null	null	null	null	null	null	null	null	null	null	null	null	20170502	22	0	default_cpm#general	null	rank#defaultset	null	null	null	-1	13	L025u31Oz5mA0pqcyMGc	902ae745c4f86877e8da4ec36e851891	36001=781.0,36057=777.0,36058=724.0,40071=438.0,40066=423.0,40014=381.0,40017=366.0,65071=300.0,36059=205.0,10063=100.64824,90006006=100.0,90002306=100.0,65410=100.0,65204=100.0,65070=100.0,40200=100.0,30009=100.0,30006=100.0,30005=100.0,15387=99.496414,16858=99.36511,40082=90.0,36105=50.0,36104=3.0,36029=3.0,36028=3.0,36027=3.0,36256=1.0,36242=1.0,10065=0.7459972	null	null"""
    messages +="""20170502094757330	[Ljava.lang.String;@74ba42ca	null	null	null	null	null	null	null	null	null	null	null	null	null	null	1	null	null	null	null	null	null	null	null	null	null	null	null	null	20170502	09	0	default_cpaorder#ctvRandom	null	rank#defaultset	null	null	null	-1	-1	L030-I1O_RyE18XbCtPI	G14CZ60byh8	8:1765#0xi040xh010xg1q0x90m0ww010wv010wu010wt0y=12600.0,8:1765:p#0xi040xh010xg1q0x90e0wt0n=10400.0,90001765=3880.0,8:1765:cp_5585#0x9080ww010wv010wu010wt0b=2200.0,8:1765:c#0x9080ww010wv010wu010wt0b=2200.0,64149=1800.0,64033=1800.0,64031=1800.0,64027=1800.0,64032=1600.0,64041=1500.0,nrtc:955:cvr|0xl00jN=1001.2287,64030=900.0,64029=900.0,64142=800.0,64028=400.0,8:1572#0wv02=200.0,64653=200.0,13677=107.10942,13496=107.10942,10683=104.0,baidu$927=100.0,baidu$92=100.0,baidu$91=100.0,baidu$89=100.0,baidu$83=100.0,baidu$506=100.0,baidu$502=100.0,baidu$494=100.0,baidu$487=100.0,baidu$442=100.0,baidu$441=100.0,baidu$399=100.0,baidu$397=100.0,baidu$393=100.0,baidu$391=100.0,baidu$386=100.0,baidu$385=100.0,baidu$378=100.0,baidu$303=100.0,baidu$291=100.0,baidu$287=100.0,baidu$275=100.0,baidu$266=100.0,baidu$263=100.0,baidu$248=100.0,baidu$202=100.0,baidu$201=100.0,baidu$199=100.0,baidu$193=100.0,baidu$147=100.0,baidu$144=100.0,90007585=100.0,600247=100.0,52306=100.0,52293=100.0,52281=100.0,52279=100.0,52137=100.0,52132=100.0,52110=100.0,52107=100.0,52096=100.0,52092=100.0,52088=100.0,52086=100.0,52082=100.0,52079=100.0,52076=100.0,52071=100.0,52044=100.0,52028=100.0,52008=100.0,52003=100.0,90001572=60.0,13800=5.0,10005=5.0,16848=0.30637318,13493=0.02582544	null	null"""
    messages +="""20170502100508400	[Ljava.lang.String;@1af9a5c	null	null	null	null	null	null	null	null	null	null	null	null	null	null	1	null	null	null	null	null	null	null	null	null	null	null	null	null	20170502	10	0	default_cpaorder#ctvRandom	null	rank#defaultset	null	null	null	-1	-1	L026HF1O_g5d08ZTgxyC	H3FAKk5duGI	90008621=619720.0,8:8621#0xm5c0xi050xh2d0xf120xe1p0xc1f0xb530xa2i0x9520x858=524200.0,8:8621#0xm5c0xi050xh2d0xf120xe1p0xc1e0xb540xa2i0x9510x858=524200.0,8:8621#0xm5b0xi060xh2c0xf130xe1o0xc1f0xb540xa2i0x9510x858=524200.0,8:8621#0xm5b0xi060xh2c0xf130xe1o0xc1f0xb530xa2i0x9520x858=524200.0,8:8621:p#0xm3h0xi030xh1e0xf0k0xe100xc0v0xb2y0xa1g0x9360x833=337800.0,8:8621:p#0xm3h0xi020xh1f0xf0k0xe0z0xc0w0xb2y0xa1f0x9370x832=337700.0,8:8621:cp_9846#0xm2a0xi020xh0z0xf0d0xe0o0xc0l0xb1l0xa0t0x9260x81x=222900.0,8:8621:cp_9846#0xm2a0xi020xh0z0xf0d0xe0n0xc0m0xb1l0xa0t0x9250x81x=222900.0,8:8621:c#0xm2a0xi020xh0z0xf0d0xe0o0xc0l0xb1l0xa0t0x9260x81x=222900.0,8:8621:c#0xm2a0xi020xh0z0xf0d0xe0n0xc0m0xb1l0xa0t0x9250x81x=222900.0,64033=1600.0,nrtc:8621:cvr|0xm0bpY0xi00ii0xh03HZ0xf016y0xe01O60xc01vy0xb00MN=1080.0868,64142=600.0,64041=400.0,64027=400.0,8:5387:cp_4913#0wy02=200.0,8:5387:c#0wy02=200.0,64031=200.0,64029=200.0,baidu$92=100.0,baidu$89=100.0,baidu$76=100.0,baidu$559=100.0,baidu$535=100.0,baidu$518=100.0,baidu$502=100.0,baidu$487=100.0,baidu$446=100.0,baidu$445=100.0,baidu$444=100.0,baidu$443=100.0,baidu$442=100.0,baidu$441=100.0,baidu$436=100.0,baidu$399=100.0,baidu$397=100.0,baidu$393=100.0,baidu$391=100.0,baidu$303=100.0,baidu$291=100.0,baidu$287=100.0,baidu$266=100.0,baidu$263=100.0,baidu$251=100.0,baidu$248=100.0,baidu$229=100.0,baidu$215=100.0,baidu$199=100.0,baidu$182=100.0,baidu$11=100.0,72085=100.0,72073=100.0,72067=100.0,72017=100.0,72009=100.0,72005=100.0,71997=100.0,71986=100.0,71974=100.0,71973=100.0,71906=100.0,71874=100.0,71870=100.0,71858=100.0,71856=100.0,71837=100.0,71834=100.0,71822=100.0,71817=100.0,71816=100.0,71812=100.0,71807=100.0,71796=100.0,71775=100.0,71766=100.0,71755=100.0,71745=100.0,71722=100.0,71714=100.0,71707=100.0,71701=100.0,71695=100.0,71682=100.0,71670=100.0,71638=100.0,71631=100.0,71620=100.0,71604=100.0,71579=100.0,71552=100.0,71551=100.0,71550=100.0,71519=100.0,71503=100.0,71477=100.0,71470=100.0,71466=100.0,71464=100.0,71454=100.0	null	null"""
    messages +="""20170502184818017	[Ljava.lang.String;@4a347128	null	null	null	null	null	null	null	null	null	null	null	null	null	null	1	null	null	null	null	null	null	null	null	null	null	null	null	null	20170502	18	0	default_cpm#general	null	rank#defaultset	null	null	null	-1	13	L1111s1P0eIr0LlnxUOD	H4KEgU8Kch4p	8:6271#0xo2e=8600.0,90006271=2670.0,rtcc:X400022405:cvr|0xo08mV=1032.1857,rtcc:X400022401:cvr|0xo05yE=1021.3889,nrtc:cluster_3274:cvr|0xo0014=1000.06616,nrtc:cluster_1828:cvr|0xo000i=1000.01984,8:2789#0xo05=500.0,90002789=150.0,13677=4.0,13496=4.0,10683=4.0,10063=4.0,10006=1.0,10005=1.0	null	null"""
    messages +="""20170502215901784	[Ljava.lang.String;@351f15a	null	null	null	null	null	null	null	null	null	null	null	null	null	null	1	null	null	null	null	null	null	null	null	null	null	null	null	null	20170502	21	0	default_cpaorder#ctvRandom	null	rank#defaultset	null	null	null	-1	-1	L0008v1O_isb0UPQG2sX	G6GKwV1AcmW1	8:1371:p#0xg0e=1400.0,8:1371#0xg0e=1400.0,65410=1400.0,64142=1400.0,nrtc:cluster_7193:cvr|0xm00sE=1001.7787,nrtc:cluster_3256:cvr|0xm0039=1000.1965,rtc:7860:cvr|0xm000r=1000.02875,nrtc:cluster_1828:cvr|0xm000r=1000.0274,nrtc:cluster_1948:cvr|0xm000i=1000.01904,nrtc:cluster_1824:cvr|0xm000c=1000.0138,nrtc:cluster_7422:cvr|0xm0006=1000.00867,64041=1000.0,64029=900.0,64031=800.0,65204=700.0,65072=700.0,64149=600.0,64033=600.0,64030=600.0,64027=600.0,8:8621#0xj010xd04=500.0,90001371=420.0,65073=400.0,90008621=180.0,10111=112.3,10110=108.1,10063=107.55539,lingji$7030025=100.0,baidu$99=100.0,baidu$96=100.0,baidu$927=100.0,baidu$92=100.0,baidu$91=100.0,baidu$90=100.0,baidu$89=100.0,baidu$88=100.0,baidu$87=100.0,baidu$85=100.0,baidu$84=100.0,baidu$83=100.0,baidu$82=100.0,baidu$790=100.0,baidu$79=100.0,baidu$76=100.0,baidu$749=100.0,baidu$748=100.0,baidu$558=100.0,baidu$535=100.0,baidu$518=100.0,baidu$508=100.0,baidu$487=100.0,baidu$445=100.0,baidu$443=100.0,baidu$442=100.0,baidu$441=100.0,baidu$425=100.0,baidu$399=100.0,baidu$397=100.0,baidu$393=100.0,baidu$391=100.0,baidu$386=100.0,baidu$385=100.0,baidu$378=100.0,baidu$377=100.0,baidu$376=100.0,baidu$375=100.0,baidu$351=100.0,baidu$343=100.0,baidu$337=100.0,baidu$319=100.0,baidu$317=100.0,baidu$314=100.0,baidu$309=100.0,baidu$303=100.0,baidu$291=100.0,baidu$287=100.0,baidu$282=100.0,baidu$275=100.0,baidu$270=100.0,baidu$266=100.0,baidu$263=100.0,baidu$262=100.0,baidu$251=100.0,baidu$248=100.0,baidu$245=100.0,baidu$239=100.0,baidu$231=100.0,baidu$229=100.0,baidu$205=100.0,baidu$204=100.0,baidu$202=100.0,baidu$201=100.0,baidu$199=100.0,baidu$193=100.0,baidu$192=100.0,baidu$182=100.0,baidu$169=100.0,baidu$148=100.0,baidu$147=100.0,baidu$144=100.0	null	null"""
    messages.foreach { mess =>
      println(parseMessage(mess))
    }

    val sparkConf = new SparkConf().setAppName("AnalysisHourly")
      .set("spark.hadoop.validateOutputSpecs", "false")
      .setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.parallelize(messages)

    //Aggregation
    val aggregationRDD = aggregation(rdd)

    //Filter
    val cacheRdd = aggregationRDD.cache()
    val rddops = new RDDOps(cacheRdd)
    //cacheRdd.saveAsTextFile("file:///D:/MySparkProjects/OptimusCombineHourly/out/result")
    val (delayRDD, normalRDD) = rddops.partitionBy { case (dayhour, line) =>
      val (day, hour) = dayTime(dayhour)
      isDelay(dayhour)
    }


    //Store
    val path = "file:///D:/MySparkProjects/OptimusCombineHourly/out/result/"
    //val path = "/tmp/OptimusCombineHourly/out/result"
    //delayRDD.saveAsNewAPIHadoopFile(path, classOf[String],classOf[String],classOf[RDDMultipleTextOutputFormat])
    delayRDD.saveAsHadoopFile(path, classOf[String], classOf[String], classOf[RDDMultipleTextOutputFormat])
    normalRDD.saveAsHadoopFile(path, classOf[String], classOf[String], classOf[RDDMultipleTextOutputFormat])

    sc.stop()

  }

  /**
    * Check delay or not
    *
    * @param dayhour
    * @return
    */
  def isDelay(dayhour: String): Boolean = {
    //val df:SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS")
    val df: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHH")
    val now = new java.util.Date
    val twoHoursTime = df.format(now.getTime - DALAY_TIMES)
    // delay in two hours.
    if (dayhour.toInt <= twoHoursTime.toInt)
      true
    else
      false
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

  /**
    * For Debug
    *
    * @param args
    */
  def main_debug(args: Array[String]): Unit = {
    val messages = scala.collection.mutable.ListBuffer[String]()
    messages += """20170426155102486	19	4187	50027	170	9900	null	9905	15	null	140x425	creative/native/2963	nativ	1156000000	1156330000	1156330600	pc	banner	fixed			9050601115763107	140	425	gdt	unknown		null								pc	na	na																null	null	null	null	null	null	null	null	1	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	20170426	15	3	default_cpm#general	null	rank#defaultset	null	null	null	null	null	L024b21P055b0E7TnAP-			M,edu_coolege,age_19_24,consuming_prower_low	gdt$M,gdt$edu_coolege,gdt$age_19_24,gdt$consuming_prower_low"""
    messages += """20170426155102040	19	4187	50027	170	9900	null	9902	15	null	140x425	creative/native/2963	nativ	1156000000	1156440000	1156440100	pc	banner	fixed			9050601115763107	140	425	gdt	unknown		null								pc	na	na																null	null	null	null	null	null	null	null	1	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	20170426	15	3	default_cpm#general	null	rank#defaultset	null	null	null	null	null	L024b21P055b0E7TJHOW			M,edu_coolege,age_0_18,consuming_prower_high	gdt$M,gdt$edu_coolege,gdt$age_0_18,gdt$consuming_prower_high"""
    messages += """20170426155102751	19	4187	50027	170	9900	null	9902	15	null	140x425	creative/native/2963	nativ	1156000000	1156350000	1156350300	pc	banner	fixed			9050601115763107	140	425	gdt	unknown		null								pc	na	na																null	null	null	null	null	null	null	null	1	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	20170426	15	3	default_cpm#general	null	rank#defaultset	null	null	null	null	null	L024b21P055b0E7U3YmP			M,age_25_34,consuming_prower_high	gdt$M,gdt$age_25_34,gdt$consuming_prower_high"""
    messages += """20170426155100496	19	4187	50027	170	9900	null	9902	15	null	140x425	creative/native/2962	nativ	1156000000	1156440000	1156440100	pc	banner	fixed			9050601115763107	140	425	gdt	unknown		null								pc	na	na																null	null	null	null	null	null	null	null	1	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	20170426	15	3	default_cpm#general	null	rank#defaultset	null	null	null	null	null	L024b21P055-0E7R-19D			M,edu_coolege,age_35_49	gdt$M,gdt$edu_coolege,gdt$age_35_49"""
    messages += """20170426155101172	19	4187	50027	170	9900	null	9905	15	null	140x425	creative/native/2962	nativ	1156000000	1156510000	1156510100	pc	banner	fixed			9050601115763107	140	425	gdt	unknown		null								pc	na	na																null	null	null	null	null	null	null	null	1	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	20170426	15	3	default_cpm#general	null	rank#defaultset	null	null	null	null	null	L024b21P055a0E7SLZv-			M,edu_coolege,age_35_49,consuming_prower_high	gdt$M,gdt$edu_coolege,gdt$age_35_49,gdt$consuming_prower_high"""
    messages += """20170426155102675	19	4187	50027	170	9900	null	9902	15	null	140x425	creative/native/2963	nativ	1156000000	1156370000	1156371400	pc	banner	fixed			9050601115763107	140	425	gdt	unknown		null								pc	na	na																null	null	null	null	null	null	null	null	1	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	20170426	15	3	default_cpm#general	null	rank#defaultset	null	null	null	null	null	L024b21P055b0E7T_F6T			F,edu_high_school,age_35_49	gdt$F,gdt$edu_high_school,gdt$age_35_49"""
    messages += """20170426155102643	19	4187	50027	170	9900	null	9902	15	null	140x425	creative/native/2962	nativ	1156000000	1156130000	1156131000	pc	banner	fixed			9050601115763107	140	425	gdt	unknown		null								pc	na	na																null	null	null	null	null	null	null	null	1	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	20170426	15	3	default_cpm#general	null	rank#defaultset	null	null	null	null	null	L024b21P055b0E7Tx4-z			M,edu_coolege,age_25_34,consuming_prower_low	gdt$M,gdt$edu_coolege,gdt$age_25_34,gdt$consuming_prower_low"""
    messages += """20170426155101201	19	4187	50027	170	9900	null	9902	15	null	140x425	creative/native/2962	nativ	1156000000	1156330000	1156330700	pc	banner	fixed			9050601115763107	140	425	gdt	unknown		null								pc	na	na																null	null	null	null	null	null	null	null	1	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	20170426	15	3	default_cpm#general	null	rank#defaultset	null	null	null	null	null	L024b21P055a0E7SNONr			F,edu_coolege,age_19_24,consuming_prower_low	gdt$F,gdt$edu_coolege,gdt$age_19_24,gdt$consuming_prower_low"""
    messages += """20170425155102481	19	4187	50027	170	9900	null	9902	15	null	140x425	creative/native/2963	nativ	1156000000	1156350000	1156350100	pc	banner	fixed			9050601115763107	140	425	gdt	unknown		null								pc	na	na																null	null	null	null	null	null	null	null	1	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	20170425	15	3	default_cpm#general	null	rank#defaultset	null	null	null	null	null	L024b21P055b0E7TmKoR			M,edu_coolege,age_25_34,consuming_prower_low	gdt$M,gdt$edu_coolege,gdt$age_25_34,gdt$consuming_prower_low"""
    messages += """20170426155100533	19	4187	50027	170	9900	null	9905	15	null	140x425	creative/native/2962	nativ	1156000000	1156500000	1156500000	pc	banner	fixed			9050601115763107	140	425	gdt	unknown		null								pc	na	na																null	null	null	null	null	null	null	null	1	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	null	20170426	15	3	default_cpm#general	null	rank#defaultset	null	null	null	null	null	L010l_1P055-0YhsnNne			F,edu_primary_school,age_35_49,consuming_prower_high	gdt$F,gdt$edu_primary_school,gdt$age_35_49,gdt$consuming_prower_high"""

    val sparkConf = new SparkConf().setAppName("AnalysisHourly")
      .set("spark.hadoop.validateOutputSpecs", "false")
      .setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.parallelize(messages)

    //Aggregation
    val aggregationRDD = aggregation(rdd)

    //Filter
    val cacheRdd = aggregationRDD.cache()
    val rddops = new RDDOps(cacheRdd)
    //cacheRdd.saveAsTextFile("file:///D:/MySparkProjects/OptimusCombineHourly/out/result")
    val (delayRDD, normalRDD) = rddops.partitionBy { case (dayhour, line) =>
      val (day, hour) = dayTime(dayhour)
      isDelay(dayhour)
    }

    //Store
    val path = "file:///D:/MySparkProjects/OptimusCombineHourly/out/result/"
    delayRDD.saveAsHadoopFile(path, classOf[String], classOf[String], classOf[RDDMultipleTextOutputFormat])
    normalRDD.saveAsHadoopFile(path, classOf[String], classOf[String], classOf[RDDMultipleTextOutputFormat])

    sc.stop()

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
  def aggregation(rdd: RDD[String]): RDD[(String, String)] = {
    val reduceRDD = rdd
      .map { line =>
        val (requestDay, hour) = dayTime(line)
        val base: RptEffectBase = parseMessage(line)
        ((base.getPartnerId, base.getAdvertiserCompanyId, base.getAdvertiserId, base.getOrderId, base.getCampaignId,
          base.getSubCampaignId, base.getExeCampaignId, base.getVerticalTagId, base.getConversionPixel, base.getCreativeSize,
          base.getCreativeId, base.getCreativeType, base.getInventoryType, base.getAdSlotType, base.getPlatform,
          requestDay + hour, base.getClientId, base.getCampaignDivisionId, base.getSubCampaignDivisionId, base.getExeCampaignDivisionId, base.getSubPlatform,
          requestDay + hour
          ),
          (base.getRaw_media_cost, base.getMedia_cost, base.getServiceFee, base.getMediaTax, base.getServiceTax,
            base.getTotalCost, base.getSystemLoss, base.getBid, base.getImp, base.getClick, base.getReach, base.getTwo_jump,
            base.getClickConversion, base.getImpConversion
            )
          )
      }.reduceByKey { (x1, x2) =>
      (x1._1 + x2._1, x1._2 + x2._2, x1._3 + x2._3, x1._4 + x2._4, x1._5 + x2._5, x1._6 + x2._6, x1._7 + x2._7,
        x1._8 + x2._8, x1._9 + x2._9, x1._10 + x2._10, x1._11 + x2._11, x1._12 + x2._12, x1._13 + x2._13, x1._14 + x2._14)
    }.map { x =>
      val (partnerId, advertiserCompanyId, advertiserId, orderId, campaignId, subCampaignId, exeCampaignId, verticalTagId,
      conversionPixel, creativeSize, creativeId, creativeType, inventoryType, adSlotType, platform,
      systemDay,client_id,campaign_division_id,sub_campaign_division_id,exe_campaign_division_id,sub_platform,daytime) =
        (x._1._1, x._1._2, x._1._3, x._1._4, x._1._5, x._1._6, x._1._7, x._1._8,
          x._1._9, x._1._10, x._1._11, x._1._12, x._1._13, x._1._14, x._1._15,
          x._1._16, x._1._17, x._1._18,  x._1._19, x._1._20, x._1._21, x._1._22
          )
      val (raw_media_cost, media_cost, serviceFee, mediaTax, serviceTax, totalCost, systemLoss, bid, imp, click, reach, two_jump, clickConversion, impConversion) =
        (x._2._1, x._2._2, x._2._3, x._2._4, x._2._5, x._2._6, x._2._7, x._2._8, x._2._9, x._2._10, x._2._11, x._2._12, x._2._13, x._2._14)

      val f_sub_platform =  if (sub_platform.isEmpty || sub_platform == "") "null" else sub_platform
      val result = s"$partnerId\t$advertiserCompanyId\t$advertiserId\t$orderId\t$campaignId\t$subCampaignId\t$exeCampaignId\t$verticalTagId" +
        s"\t$conversionPixel\t$creativeSize\t$creativeId\t$creativeType\t$inventoryType\t$adSlotType\t$platform" +
        s"\t$raw_media_cost\t$media_cost\t$serviceFee\t$mediaTax\t$serviceTax\t$totalCost\t$systemLoss\t$bid\t$imp\t$click\t$reach\t$two_jump\t$clickConversion\t$impConversion\t$systemDay" +
        s"\t$client_id\t$campaign_division_id\t$sub_campaign_division_id\t$exe_campaign_division_id\t$f_sub_platform"

      (daytime, result)
    }
    reduceRDD
  }

  def aggregation(rdd: DStream[String]): DStream[(String, String)] = {
    val reduceRDD = rdd
      .map { line =>
        val (requestDay, hour) = dayTime(line)
        val base: RptEffectBase = parseMessage(line)
        ((base.getPartnerId, base.getAdvertiserCompanyId, base.getAdvertiserId, base.getOrderId, base.getCampaignId,
          base.getSubCampaignId, base.getExeCampaignId, base.getVerticalTagId, base.getConversionPixel, base.getCreativeSize,
          base.getCreativeId, base.getCreativeType, base.getInventoryType, base.getAdSlotType, base.getPlatform, requestDay + hour, requestDay + hour,
          base.getClientId, base.getCampaignDivisionId, base.getSubCampaignDivisionId, base.getExeCampaignDivisionId, base.getSubPlatform),
          (base.getRaw_media_cost, base.getMedia_cost, base.getServiceFee, base.getMediaTax, base.getServiceTax,
            base.getTotalCost, base.getSystemLoss, base.getBid, base.getImp, base.getClick, base.getReach, base.getTwo_jump,
            base.getClickConversion, base.getImpConversion
            )
          )
      }.reduceByKey { (x1, x2) =>
      (x1._1 + x2._1, x1._2 + x2._2, x1._3 + x2._3, x1._4 + x2._4, x1._5 + x2._5, x1._6 + x2._6, x1._7 + x2._7,
        x1._8 + x2._8, x1._9 + x2._9, x1._10 + x2._10, x1._11 + x2._11, x1._12 + x2._12, x1._13 + x2._13, x1._14 + x2._14)
    }.map { x =>
      val (partnerId, advertiserCompanyId, advertiserId, orderId, campaignId, subCampaignId, exeCampaignId, verticalTagId,
      conversionPixel, creativeSize, creativeId, creativeType, inventoryType, adSlotType, platform, systemDay, daytime,
      client_id,campaign_division_id,sub_campaign_division_id,exe_campaign_division_id,sub_platform
        ) = (x._1._1, x._1._2, x._1._3, x._1._4, x._1._5, x._1._6, x._1._7, x._1._8,
        x._1._9, x._1._10, x._1._11, x._1._12, x._1._13, x._1._14, x._1._15, x._1._16, x._1._17,
        x._1._18, x._1._19, x._1._20, x._1._21, x._1._22
        )
      val (raw_media_cost, media_cost, serviceFee, mediaTax, serviceTax, totalCost, systemLoss, bid, imp, click, reach, two_jump, clickConversion, impConversion) =
        (x._2._1, x._2._2, x._2._3, x._2._4, x._2._5, x._2._6, x._2._7, x._2._8, x._2._9, x._2._10, x._2._11, x._2._12, x._2._13, x._2._14)

      val f_sub_platform =  if (sub_platform.isEmpty || sub_platform == "") "null" else sub_platform
      val result = s"$partnerId\t$advertiserCompanyId\t$advertiserId\t$orderId\t$campaignId\t$subCampaignId\t$exeCampaignId\t$verticalTagId" +
        s"\t$conversionPixel\t$creativeSize\t$creativeId\t$creativeType\t$inventoryType\t$adSlotType\t$platform" +
        s"\t$raw_media_cost\t$media_cost\t$serviceFee\t$mediaTax\t$serviceTax\t$totalCost\t$systemLoss\t$bid\t$imp\t$click\t$reach\t$two_jump\t$clickConversion\t$impConversion\t$systemDay" +
        s"\t$client_id\t$campaign_division_id\t$sub_campaign_division_id\t$exe_campaign_division_id\t$f_sub_platform"

      (daytime, result)
    }
    reduceRDD
  }

  //val file = new java.io.File("conf/application.conf")
  //val config = ConfigFactory.load//ConfigFactory.parseFile(new java.io.File("conf/application.conf"))
  val config = ConfigFactory.parseFile(new java.io.File("conf/kafka2palo.conf"))


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

  def writeDataByTable(rdd: RDD[(String, String)],
                       table_or_path: String,
                       schema: StructType,
                       sqlContext: HiveContext): Unit = {
    val numberOfFilesPerBatch = config.getInt("spark.numFilesPerBatch")
    val data = rdd.map { event =>
      //      println("event._1: " + event._1.toString + " ||| event._2: " + event._2.toString + "^^^ length:" + event._2.split("\t").length)
      val (rday, rhour) = {
        (event._1.substring(0, 8), event._1.substring(8))
      }
      Row.merge(Row.fromSeq(event._2.split(SEPERATOR)), Row.fromSeq(Seq(rday, rhour)))
    }.repartition(numberOfFilesPerBatch)

    val df = sqlContext.createDataFrame(data, schema)

//    df.write.format("orc").mode(SaveMode.Append).partitionBy("pday", "phour")
//      .saveAsTable(table)
    val date = new Date();
    val formatter = new SimpleDateFormat ("yyyyMMddHHmm");
    val timestr = formatter.format(date)
    val path = table_or_path + timestr
    val commands = "hadoop fs -rmr " + path + ""!

    df.map(_.mkString("\t")).saveAsTextFile(path)

    val po = new PaloOperator().execLoad(timestr)
  }

  def processRDD(rdd: RDD[(String, String, String)])(implicit sqlContext: HiveContext): Unit = {
    val message = rdd.map(_._2)

    val aggregationRDD = aggregation(message)
//    val cacheRdd = aggregationRDD.cache()
//    val dsOps = new RDDOps(cacheRdd)
//    val (delayRDD, normalRDD) = dsOps.partitionBy { case (dayhour, line) =>
//      isDelay(dayhour)
//    }
    val schemaString = (1 to 35).map(i => "C" + i).mkString("\t")
    val dayhour = "\tpday\tphour"
    val schema = StructType(
      (schemaString + dayhour).split("\t").map(fieldName => StructField(fieldName, StringType, true))
    )
    //    println("schema:toString" + schema.toString())
//    writeDataByTable(aggregationRDD, config.getString("spark.day_table"), schema, sqlContext)
    writeDataByTable(aggregationRDD, config.getString("spark.result_path"), schema, sqlContext)
//    writeDataByTable(delayRDD, config.getString("spark.delay"), schema, sqlContext)
  }

  def processRDD_pre(rdd: RDD[(String, String, String)])(implicit sqlContext: HiveContext): Unit = {
    val message = rdd.map(_._2).filter(isValidData(_))

    val aggregationRDD = aggregation(message)
    val cacheRdd = aggregationRDD.cache()
    val dsOps = new RDDOps(cacheRdd)
    val (delayRDD, normalRDD) = dsOps.partitionBy { case (dayhour, line) =>
      isDelay(dayhour)
    }
    val schemaString = (1 to 34).map(i => "C" + i).mkString(SEPERATOR)
    val dayhour = "\tpday\tphour"
    val schema = StructType(
      (schemaString + dayhour).split(SEPERATOR).map(fieldName => StructField(fieldName, StringType, true))
    )

    writeDataByTable(normalRDD, "default.hw_rpt_effect_day", schema, sqlContext)
    writeDataByTable(delayRDD, "default.hw_delay_rpt_effect_day", schema, sqlContext)
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


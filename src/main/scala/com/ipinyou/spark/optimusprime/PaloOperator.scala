package com.ipinyou.spark.optimusprime

/**
  * Created by giant on 17/8/19.
  */

import org.apache.spark.sql._
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.ListBuffer
import java.sql.DriverManager

class PaloOperator {

  def execLoad(lable:String):Unit = {
    var conn = DriverManager.getConnection("jdbc:mysql://192.168.163.182:9030", "root@palo_cluster", "root")
    var stmt = conn.createStatement()
    stmt.execute("enter palo_cluster;")
    stmt.execute("use warehouse;")
    val loadDataSql =
      s"""
         |LOAD LABEL rpt_effect_hour$lable
         |(
         |    DATA INFILE(\"hdfs://namenode16011/tmp/kafka2palo/rpt_palo_hour/time=$lable/*\")
         |    INTO TABLE `rpt_effect_hour`
         |    COLUMNS TERMINATED BY \"\\t\"
         |    (partner_id,advertiser_company_id,advertiser_id,order_id,campaign_id,sub_campaign_id,exe_campaign_id,vertical_tag_id,conversion_pixel,creative_size,creative_id,creative_type,inventory_type,ad_slot_type,platform,raw_media_cost,media_cost,service_fee,media_tax,service_tax,total_cost,system_loss,bid,imp,click,reach,two_jump,click_conversion,imp_conversion,day,client_id,campaign_division_id,sub_campaign_division_id,exe_campaign_division_id,sub_platform,pday,phour)
         |) WITH BROKER hdfs_broker;
       """.stripMargin

    println(loadDataSql)
    stmt.execute(loadDataSql)
    stmt.close()
  }


  def execLoadBase(lable:String):Unit = {
    var conn = DriverManager.getConnection("jdbc:mysql://192.168.163.182:9030", "root@palo_cluster", "root")
    var stmt = conn.createStatement()
    stmt.execute("enter palo_cluster;")
    stmt.execute("use warehouse;")
    val loadDataSql =
      s"""
         |LOAD LABEL rpt_effect_base$lable
         |(
         |    DATA INFILE(\"hdfs://namenode16011/tmp/kafka2palo/rpt_palo_base/time=$lable/*\")
         |    INTO TABLE `rpt_effect_base`
         |    COLUMNS TERMINATED BY \"\\t\"
         |    (advertiser_id,pday,partner_id,advertiser_company_id,order_id,campaign_id,sub_campaign_id,exe_campaign_id,vertical_tag_id,conversion_pixel,creative_size,creative_id,creative_type,country_id,province_id,city_id,inventory_type,ad_slot_type,banner_view_type,video_view_type,native_view_type,ad_unit_id,ad_unit_width,ad_unit_height,platform,domain_category,top_level_domain,domain,app_category,app_name,app_id,deal_type,deal_id,device_type,os,browser,brand,model,network_generation,carrier,mob_device_type,client_id,campaign_division_id,sub_campaign_division_id,exe_campaign_division_id,sub_platform,phour,raw_media_cost,media_cost,service_fee,media_tax,service_tax,total_cost,system_loss,bid,imp,click,reach,two_jump,click_conversion,imp_conversion)
         |) WITH BROKER hdfs_broker ("exe_mem_limit"="6442450944");
       """.stripMargin

    println(loadDataSql)
    stmt.execute(loadDataSql)
    stmt.close()
  }

}

object PaloOperator {

  def main(args: Array[String]): Unit = {

//    var conn = DriverManager.getConnection("jdbc:mysql://192.168.163.182:9030", "root@palo_cluster", "root")
//    var stmt = conn.createStatement()
//    stmt.execute("enter palo_cluster;")
//    var selectSql = "show load where state='ETL';"
//    var lable = "201708311727"
//    stmt.execute("use warehouse;")
//    val loadDataSql =
//      s"""
//         |LOAD LABEL rpt_effect_hour_t1_$lable
//         |(
//         |    DATA INFILE(\"hdfs://namenode16011/tmp/kafka2palo/rpt_palo_hour/time=$lable/*\")
//         |    INTO TABLE `rpt_effect_hour`
//         |    COLUMNS TERMINATED BY \"\\t\"
//         |    (partner_id,advertiser_company_id,advertiser_id,order_id,campaign_id,sub_campaign_id,exe_campaign_id,vertical_tag_id,conversion_pixel,creative_size,creative_id,creative_type,inventory_type,ad_slot_type,platform,raw_media_cost,media_cost,service_fee,media_tax,service_tax,total_cost,system_loss,bid,imp,click,reach,two_jump,click_conversion,imp_conversion,day,client_id,campaign_division_id,sub_campaign_division_id,exe_campaign_division_id,sub_platform,pday,phour)
//         |) WITH BROKER hdfs_broker;
//       """.stripMargin
//
//    stmt.execute(loadDataSql)
//    stmt.close()
    new PaloOperator().execLoadBase("201709052154")

  }
}
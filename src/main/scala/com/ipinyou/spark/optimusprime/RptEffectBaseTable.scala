package com.ipinyou.spark.optimusprime

/**
  * Created by giant on 17/9/4.
  */
class RptEffectBaseTable(var advertiser_id:Long, var pday:Long, var partner_id:Long, var advertiser_company_id:Long,
                         var order_id:Long, var campaign_id:Long, var sub_campaign_id:Long, var exe_campaign_id:Long,
                         var vertical_tag_id:Long, var conversion_pixel:Long, var creative_size:String, var creative_id:String,
                         var creative_type:String, var country_id:Long, var province_id:Long, var city_id:Long,
                         var inventory_type:String, var ad_slot_type:String, var banner_view_type:String, var video_view_type:String,
                         var native_view_type:String, var ad_unit_id:String, var ad_unit_width:Long, var ad_unit_height:Long,
                         var platform:String, var domain_category:String, var top_level_domain:String, var domain:String,
                         var app_category:String, var app_name:String, var app_id:String, var deal_type:String, var deal_id:String,
                         var device_type:String, var os:String,
                         var browser:String, var brand:String, var model:String, var network_generation:String, var carrier:String,
                         var mob_device_type:String, var mac:String, var mac_enc:String, var imei:String, var imei_enc:String,
                         var imsi:String, var imsi_enc:String, var dpid:String, var dpidenc:String, var adid:String,
                         var adid_enc:String, var client_id:Long, var campaign_division_id:Long, var sub_campaign_division_id:Long,
                         var exe_campaign_division_id:Long, var sub_platform:String, var phour:Long, var raw_media_cost:Double,
                         var media_cost:Double, var service_fee:Double, var media_tax:Double, var service_tax:Double,
                         var total_cost:Double, var system_loss:Double, var bid:Long, var imp:Long, var click:Long,
                         var reach:Long, var two_jump:Long, var click_conversion:Long, var imp_conversion:Long){


  def this() {
//    this(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, "", "", "", 0L, 0L, 0L, "", "", "", "", "", "", 0L, 0L, "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 0L, 0L, 0L, 0L, "", 0L, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0L, 0L, 0L, 0L, 0L, 0L, 0L)
    this(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, null, null, null, 0L, 0L, 0L, null, null, null, null, null, null, 0L, 0L, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, 0L, 0L, 0L, 0L, null, 0L, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0L, 0L, 0L, 0L, 0L, 0L, 0L)
  }

  def dimToString():String = {
    var fieldBuffer = new scala.collection.mutable.ListBuffer[Any]()
    fieldBuffer.append(advertiser_id)
    fieldBuffer.append(pday)
    fieldBuffer.append(partner_id)
    fieldBuffer.append(advertiser_company_id)
    fieldBuffer.append(order_id)
    fieldBuffer.append(campaign_id)
    fieldBuffer.append(sub_campaign_id)
    fieldBuffer.append(exe_campaign_id)
    fieldBuffer.append(vertical_tag_id)
    fieldBuffer.append(conversion_pixel)
    fieldBuffer.append(creative_size)
    fieldBuffer.append(creative_id)
    fieldBuffer.append(creative_type)
    fieldBuffer.append(country_id)
    fieldBuffer.append(province_id)
    fieldBuffer.append(city_id)
    fieldBuffer.append(inventory_type)
    fieldBuffer.append(ad_slot_type)
    fieldBuffer.append(banner_view_type)
    fieldBuffer.append(video_view_type)
    fieldBuffer.append(native_view_type)
    fieldBuffer.append(ad_unit_id)
    fieldBuffer.append(ad_unit_width)
    fieldBuffer.append(ad_unit_height)
    fieldBuffer.append(platform)
    fieldBuffer.append(domain_category)
    fieldBuffer.append(top_level_domain)
    fieldBuffer.append(domain)
    fieldBuffer.append(app_category)
    fieldBuffer.append(app_name)
    fieldBuffer.append(app_id)
    fieldBuffer.append(deal_type)
    fieldBuffer.append(deal_id)
    fieldBuffer.append(device_type)
    fieldBuffer.append(os)
    fieldBuffer.append(browser)
    fieldBuffer.append(brand)
    fieldBuffer.append(model)
    fieldBuffer.append(network_generation)
    fieldBuffer.append(carrier)
    fieldBuffer.append(mob_device_type)
//    fieldBuffer.append(mac)
//    fieldBuffer.append(mac_enc)
//    fieldBuffer.append(imei)
//    fieldBuffer.append(imei_enc)
//    fieldBuffer.append(imsi)
//    fieldBuffer.append(imsi_enc)
//    fieldBuffer.append(dpid)
//    fieldBuffer.append(dpidenc)
//    fieldBuffer.append(adid)
//    fieldBuffer.append(adid_enc)
    fieldBuffer.append(client_id)
    fieldBuffer.append(campaign_division_id)
    fieldBuffer.append(sub_campaign_division_id)
    fieldBuffer.append(exe_campaign_division_id)
    fieldBuffer.append(sub_platform)
    fieldBuffer.append(phour)

    fieldBuffer.mkString("\t")
  }
}

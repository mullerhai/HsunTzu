package com.HsunTzu.utils

import com.typesafe.scalalogging.Logger
import io.circe.{Json, JsonObject}
import io.circe._
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import org.slf4j.LoggerFactory

class JsonUtils {

}

object JsonUtils {

  private [this] val jsonLog=Logger(LoggerFactory.getLogger(classOf[JsonUtils]))
  /**
    * 判断 json 对象中 是否 有 对应的key
    *
    * @param metrix
    * @param key
    * @return
    */
  def boolJsonHasKey(metrix: Json, key: String): Boolean = {
    if (metrix.findAllByKey(key).size > 0) {
      return true
    } else {
      return false
    }
  }

  /**
    * 通过字符串 解析为字符串数组 获取 json 对象
    *
    * @param metrix
    * @param index
    * @param delimiter
    * @return
    */
  def getJsonFromStrPartArray(metrix: String, index: Int)(delimiter: String = "\t"): Json = {

    val jsonStr = metrix.split(delimiter)(index)
    var jsonObj: Json = null
    try {
      jsonObj = parser.parse(jsonStr).getOrElse(Json).asInstanceOf[Json]
    } catch {
      case e: Exception => e.printStackTrace()
    }
    return jsonObj
  }

  /**
    * 通过字符串获取 json 对象
    *
    * @param metricx
    * @return
    */
  def getJsonFromStr(metricx: String): Json = {
    var jsonObj: Json = null
    try {
      jsonObj = parser.parse(metricx).getOrElse(Json).asInstanceOf[Json]
    } catch {
      case e: Exception => e.printStackTrace()
    }
    return jsonObj
  }

  /**
    * 解析  json 获取 key 值，适合单次 使用，批量使用 性能损耗较大
    *
    * @param metricx
    * @param key
    * @return
    */
  def getValuesFromStr(metricx: String, key: String): List[Json] = {
    val defaultVal: List[Json] = List("-".asJson)
    var jsonObj: Json = null
    try {
      jsonObj = parser.parse(metricx).getOrElse(Json).asInstanceOf[Json]
      if (jsonObj.findAllByKey(key).size != 0) {
        return jsonObj.findAllByKey(key)
      } else {
        return defaultVal
      }
    } catch {
      case e: Exception => return defaultVal
    }

  }

  /**
    * 通过 json 对象 解析 key 值 ，减少性能损耗，适合 批量操作
    *
    * @param jsonObj
    * @param key
    * @return
    */
  def getValuesFromJson(jsonObj: Json, key: String): List[Json] = {
    val defaultVal: List[Json] = List("-".asJson)
    try {
      if (jsonObj.findAllByKey(key).size != 0) {
        return jsonObj.findAllByKey(key)
      } else {
        return defaultVal
      }
    } catch {
      case e: Exception => return defaultVal
    }

  }

  def getStrValuesFromJson(jsonObj: Json, key: String): String = {
    val defaultVal: String = "-"
    try {
      if (jsonObj.findAllByKey(key).size != 0) {
        return jsonObj.findAllByKey(key).toString()
      } else {
        return defaultVal
      }
    } catch {
      case e: Exception => return defaultVal
    }

  }

  val simpleJsonString =
    """
    {
      "name": "Deathstroke",
      "alias": ["Slade Wilson"],
      "active": true,
      "firstApparition": 1980,
      "cover": "https://upload.wikimedia.org/wikipedia/en/thumb/6/67/Deathstroke_Vol_2_8.png/250px-Deathstroke_Vol_2_8.png",
      "habilities": [
        "Genius level intellect",
        "Skilled manipulator and deceiver",
        "Increased superhuman"
      ],
      "publisher": {
        "name": "DC Comics",
        "authors": ["Marv Wolfman", "George Pérez"]
      }
    }
  """

  val originlog =
    """
      |2017-12-25 01:00:00	ctrlog	{"ctrRequest": {"ad_request": {"sid": "6a58fd4dc93f0143_4000026","server_ip": "221.228.204.106","test": 0,"device_info": {"idfa": "","idfa_md5": "","imei": "99000810443823","imei_md5": "83a246099b45e760c2a963604024bbec","android_id": "1b7142bd9085fca3","android_id_md5": "391c16478350236259af12bb407c3c33","os": "android","osv": "6.0","w": 1080,"h": 1920,"ppi": 0,"mac": "","make": "xiaomi","model": "redmi_note_4","connection_type": "NT_UnKnown","carrier": "UnKnown","client_ip": "139.207.193.35","ua": "Mozilla/5.0 (Linux; Android 6.0; Redmi Note 4 Build/MRA58K; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/46.0.2490.76 Mobile Safari/537.36","lon": 0.0,"lat": 0.0},"app_info": {"app_name": "","app_version": "60271066"},"user_info": {"user_id": "","age": "0","gender": "M","user_tag": ""}},"media_app_info": {"media_app_id": 4000026,"linkedme_key": "3605f55d6dd5468f33459098812d0d17","media_cat": "工具","media_cat_black_list": "","media_ad_app_black_list": ""},"ad_pos_info": {"ad_pos_id": "4000026_65","is_multi_img": false,"low_price": 20.0,"ad_weigh": "{\"8000023_21\":10,\"8000013_23\":9,\"8000047_33\":8,\"11236_0\":7}","scale": "1200X628","impr_limit_count": 0,"impr_control_flag": 1},"timestamp": 1514134799},"adResponse": {"ad_plan_list": [{"adid": "8000023_21","ad_app_info": {"ad_app_id": 8000023,"ad_app_name": "","pkg_name": "com.sohu.newsclient","ad_cat": "资讯"},"check_install_status": 1,"ad_content_list": [{"cid": "1330","deeplink": "sohunews://pr/channel://channelId=13557&newsId=249158963&startfrom=4504deeplink&ignoreLoadingAd=1","img": "http://img.lkme.cc/1330/1200X628","title": "儿子抽中亿元大奖 父亲买凶杀人想独吞","sub_title": "上搜狐新闻，知天下大事","ctr_val": 0.0,"rank_score": 0.0,"cpm": 20.0}]}]}}
    """.stripMargin
  val sjsctrlog =
    """
            {"ctrRequest": {"ad_request": {"sid": "d144e5bcee03a09d_4000026","server_ip": "221.228.204.104","test": 0,"device_info": {"idfa": "","idfa_md5": "","imei": "A0000055A8F5E9","imei_md5": "acd63aeee15c4c94ca7da35a340bffb3","android_id": "6371c48bf58360b7","android_id_md5": "fd51bbbf028720e731cd32ca70c74ed6","os": "android","osv": "4.4","w": 1280,"h": 720,"ppi": 0,"mac": "","make": "huawei","model": "huawei_honor-4x","connection_type": "NT_UnKnown","carrier": "CTCC","client_ip": "106.17.183.46","ua": "Mozilla/5.0 (Linux; Android 4.4.4; Che1-CL10 Build/Che1-CL10) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/33.0.0.0 Mobile Safari/537.36","lon": 0.0,"lat": 0.0},"app_info": {"app_name": "","app_version": "60271066"},"user_info": {"user_id": "","age": "0","gender": "O","user_tag": ""}},"media_app_info": {"media_app_id": 4000026,"linkedme_key": "3605f55d6dd5468f33459098812d0d17","media_cat": "工具","media_cat_black_list": "","media_ad_app_black_list": ""},"ad_pos_info": {"ad_pos_id": "4000026_65","is_multi_img": false,"low_price": 20.0,"ad_weigh": "{\"8000023_21\":10,\"8000013_23\":9,\"8000047_33\":8,\"11236_0\":7}","scale": "1200X628","impr_limit_count": 0,"impr_control_flag": 1},"timestamp": 1514134799},"adResponse": {}}
    """.stripMargin

  def main(args: Array[String]): Unit = {
    val sm = Json.obj("name" -> "haining".asJson,
      "gender" -> "man".asJson,
      "birth" -> "2017".asJson
    )
    val sss = parser.parse(sjsctrlog)
    val ssoo = getJsonFromStr(simpleJsonString)
    val ooo = getValuesFromJson(ssoo, "habilities")
    val ssxxx = getValuesFromStr(sjsctrlog, "ad_content_list")
    val ssx: Json = sss.getOrElse(Json).asInstanceOf[Json]
    val sso = ssx.findAllByKey("imei_md5")(0)
    val sm2 = Json.fromString(sjsctrlog)
    val fdx = getJsonFromStrPartArray(originlog, 2)("\t")
    val wwt = getValuesFromJson(fdx, "deeplink")
    val fsx = originlog.split("\t")(2)
    println(fdx)
    println(wwt)


  }
}


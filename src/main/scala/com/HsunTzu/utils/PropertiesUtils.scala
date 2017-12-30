package com.HsunTzu.utils

import java.io.{File, FileInputStream, InputStream}
import java.util.Properties

import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class PropertiesUtils {

}

object PropertiesUtils{


  private [this]val proLogger= Logger(LoggerFactory.getLogger(classOf[PropertiesUtils]))

  /** *
    * jar包外 的配置文件 读取
    *
    * @param filePath          jar包外 的配置文件 读取目录路径,
    * @param propertieskeyname 属性文件的 key，为null 时，则读取全部的key，逗号分隔
    * @param   delimiter       分隔符 默认逗号
    * @return
    */
  def converOuterPropertiesToSeq(filePath: String, propertieskeyname: String*)(delimiter: String = ","): ArrayBuffer[String] = {

    val pro: Properties = new Properties()
    var seqPro: ArrayBuffer[String] = mutable.ArrayBuffer()
    try {
      val ins: InputStream = new FileInputStream(new File(filePath))
      pro.load(ins)
    } catch {
      case e: Exception => throw e
    }
    if (propertieskeyname.length < 1) {
      val proEmu = pro.propertyNames()
      while (proEmu.hasMoreElements) {
        val fliePrefix: String = pro.getProperty(proEmu.nextElement().toString)
        if (!fliePrefix.contains(delimiter)) {
          seqPro.+=(fliePrefix)
        } else {
          fliePrefix.split(delimiter).foreach(x => {
            seqPro.+=(x)
          })
        }

      }
    } else {
      propertieskeyname.foreach(key => {
        val seqvals = pro.getProperty(key)
        seqvals.split(",").foreach(v => {
          seqPro.+=(v)
        })
      })
    }

    return seqPro
  }




  /**
    * jar包内的资源文件目录读取,按key  读取字段，逗号分隔,没有输入字段就分key 读取
    *
    * @param filePath          jar包内的资源文件目录路径,
    * @param propertiesKeyname 属性文件的 key，为null 时，则读取全部的key，逗号分隔
    * @param   delimiter       分隔符 默认逗号
    * @return 属性文件 value 序列
    */
  def converInnerPropertiesToSeq(filePath: String, propertiesKeyname: String*)(delimiter: String = ","): ArrayBuffer[String] = {
    var seqPro: ArrayBuffer[String] = mutable.ArrayBuffer()
    val pro: Properties = new Properties()
    try {
      val pertiesPath = this.getClass.getClassLoader.getResourceAsStream(filePath)
      pro.load(pertiesPath)
    } catch {
      case e: NullPointerException => throw e
    }
    if (propertiesKeyname.length < 1) {
      val proEmu = pro.propertyNames()
      while (proEmu.hasMoreElements) {
        val fliePrefix: String = pro.getProperty(proEmu.nextElement().toString)
        if (!fliePrefix.contains(delimiter)) {
          seqPro.+=(fliePrefix)
        } else {
          fliePrefix.split(delimiter).foreach(x => {
            seqPro.+=(x)
          })
        }

      }
    } else {
      propertiesKeyname.foreach(key => {
        val proVals = pro.getProperty(key)
        proVals.split(delimiter).foreach(vas => {
          seqPro.+=(vas)
        })
      })
    }
    return seqPro
  }

}
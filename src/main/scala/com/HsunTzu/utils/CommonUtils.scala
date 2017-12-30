package com.HsunTzu.utils

import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks

class CommonUtils {

}
object  CommonUtils{

  private[this] val logger =Logger(LoggerFactory.getLogger(classOf[CommonUtils]))

  /**
    * 获取文件名称 通过路径
    *
    * @param path
    * @return
    */
  def getFileName(path: String): String = {
    val tmps: Array[String] = path.split("/")
    val tmsize = tmps.length
    var fileName: String = tmps(tmsize - 1)
    if(fileName.contains(".")){
      fileName = fileName.substring(0, fileName.lastIndexOf("."))
    }
    return fileName
  }

  /**
    * 判断文件前缀
    *
    * @param tarEntryName
    * @return
    */
  def boolFilePrefixContains(tarEntryName: String,propertiesPath:String): Boolean = {
    var flag:Boolean=false
    val loop: Breaks = new Breaks
    val prefix:String="files"
    val delimiter:String=","
    val proSeq: ArrayBuffer[String] = CompressUtils.converOuterPropertiesToSeq(propertiesPath,prefix)(delimiter)
    loop.breakable {
      proSeq.foreach(fliePrefix => {
        val suffixContain = tarEntryName.contains(fliePrefix)
        logger.info("bool fliePrefix || " + fliePrefix + " res  || " + suffixContain)
        if (suffixContain == true) {
          logger.info(" suffixContain   && " + suffixContain)
          flag = true
          loop.break()
        }
      })
    }
    return flag
  }


  /** *
    * 由原始文件的二级目录 获取输出目录
    *
    * @param path
    * @return
    */
  def getOutFileSubPath(path: String): String = {
    var nPath: String = ""
    var subPath = ""
    var subindex: Int = 0
    if (path.trim.startsWith("/")) {
      nPath = path.substring(1)
      subindex = nPath.trim.indexOf("/")
      subPath = nPath.substring(subindex)
    } else {
      subindex = path.trim.indexOf("/")
      subPath = path.substring(subindex)
    }
    return subPath
  }
}

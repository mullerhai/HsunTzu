package com.HsunTzu.hdfs

import com.typesafe.scalalogging.Logger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress._
import org.slf4j.LoggerFactory

class HdfsCodec {

}

object HdfsCodec{

  private [this] val logger =Logger(LoggerFactory.getLogger(classOf[HdfsCodec]))
  /**
    * 压缩格式 数字信号 到压缩格式 的映射
    *
    * @param signal
    * @return
    */
  def codecSignalToCodec(signal: String): String = signal.trim match {
    case "0" => "DEFLATE"
    case "1" => "SNAPPY"
    case "2" => "GZIP"
    case "3" => "LZO"
    case "4" => "BZIP2"
    case "5" => "DEFAULT"
    case _ =>  "DEFLATE"
  }

  /**
    * 根据 codec 字符串 创建 codec格式
    * @param codecStr
    * @return
    */
  def codecStrToCodec(codecStr:String):CompressionCodec=codecStr.trim match {
    case "SNAPPY" => new SnappyCodec()
    case "GZIP" => new GzipCodec()
    case "LZO" => new Lz4Codec()
    case  "BZIP2" => new BZip2Codec()
    case "DEFLATE" => new DeflateCodec()
    case  "DEFAULT" => new DefaultCodec()
    case _ =>  new DeflateCodec()
  }

  /**
    * 根据 压缩格式 设置 conf
    * @param codecStr
    * @param codecTrait
    * @param conf
    */
  def codecTosetConf(codecStr:String,codecTrait:CompressionCodec,conf:Configuration):Unit=codecStr.trim match {
    case "SNAPPY" => codecTrait.asInstanceOf[SnappyCodec].setConf(conf)
    case "GZIP"   =>  codecTrait.asInstanceOf[GzipCodec].setConf(conf)
    case "LZO"    => codecTrait.asInstanceOf[Lz4Codec].setConf(conf)
    case "BZIP2"  => codecTrait.asInstanceOf[BZip2Codec].setConf(conf)
    case "DEFLATE" => codecTrait.asInstanceOf[DeflateCodec].setConf(conf)
    case "DEFAULT" => codecTrait.asInstanceOf[DefaultCodec].setConf(conf)
    case  _         => codecTrait.asInstanceOf[DeflateCodec].setConf(conf)
  }

}
package com.HsunTzu.core

import java.io.{BufferedInputStream, BufferedOutputStream}

import com.HsunTzu.hdfs.HdfsCodec
import com.HsunTzu.utils.{CommonUtils, HdfsUtils}
import com.typesafe.scalalogging.Logger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.io.compress.{CompressionCodec, CompressionInputStream, CompressionOutputStream}
import org.slf4j.LoggerFactory

class HdfsConvertCompress {

}

object HdfsConvertCompress{

  private [this] val logger=Logger(LoggerFactory.getLogger(classOf[HdfsConvertCompress]))
  val HDFSPORT="9000"
  val HDFSPORTDOTSUFFIX=":"+HDFSPORT+"/"


  /***
    * 目录级别 从一种压缩格式 到另一种 压缩格式
    * @param fs
    * @param conf
    * @param inpath
    * @param outPath
    * @param inputCodec
    * @param outputCodec
    * @param propertiesPath
    */
  def dirConverterOneCompressToOtherCompress(fs: FileSystem, conf: Configuration, inpath: String, outPath: String, inputCodec: String,outputCodec:String)(propertiesPath:String):Unit={

    val inputPa: Path = new Path(inpath)
    val fsStatus: FileStatus = fs.getFileStatus(inputPa)
    var flag = false
    try{
      if(fsStatus.isFile){
        flag =CommonUtils.boolFilePrefixContains(inpath, propertiesPath)
        if (flag) {
          converterOneCompressToOtherCompress(fs, conf, inpath, outPath,inputCodec, outputCodec)(propertiesPath)
        }
      }else if (fsStatus.isDirectory){
        val fslist:Array[FileStatus]=fs.listStatus(inputPa)
        fslist.foreach(fins=>{
          val newInp =HdfsUtils.getNewSubInpathByFileStatus(fins)
          if (fins.isFile) {
            flag = CommonUtils.boolFilePrefixContains(newInp, propertiesPath)
            if (flag) {
              logger.info(s"newsubInpath ${newInp}  outPath ${outPath}  input Codec ${inputCodec}  outputCodec  ${outputCodec}")
              converterOneCompressToOtherCompress(fs,conf,newInp,outPath,inputCodec,outputCodec)(propertiesPath)
            }
          }else{
            logger.warn("sencond loop")
            dirConverterOneCompressToOtherCompress(fs,conf,newInp,outPath,inputCodec,outputCodec)(propertiesPath)
          }

        })
      }
    }catch {
      case e:Exception => e.printStackTrace()
    }finally {
      //fs.close()
    }

  }

  /***
    * 转换压缩方式 从一种压缩到另一压缩  雏形
    * @param fs
    * @param conf
    * @param inpath
    * @param outPath
    * @param inputCodec "SNAPPY" "GZIP" "LZO"  "DEFALTE"
    * @param outputCodec  "SNAPPY" "GZIP" "LZO"  "DEFALTE"
    */
  def  converterOneCompressToOtherCompress(fs: FileSystem, conf: Configuration, inpath: String, outPath: String, inputCodec: String,outputCodec:String)(propertiesPath:String="/usr/local/info.properties"):Unit={
    val inputPath: Path = new Path(inpath)
    val inFSData: FSDataInputStream = fs.open(inputPath)
    var readlen = 0
    val ioBuffer: Array[Byte] = new Array[Byte](60 * 1024)
    if(inputCodec.trim==outputCodec.trim){
      return
    }
    val codecFirst:CompressionCodec =HdfsCodec.codecStrToCodec(inputCodec)
    HdfsCodec.codecTosetConf(inputCodec,codecFirst,conf)
    val codecSecond:CompressionCodec =HdfsCodec.codecStrToCodec(outputCodec)
    HdfsCodec.codecTosetConf(outputCodec,codecSecond,conf)
    val secondcompresFile= HdfsUtils.dropExtensionGetOutHdfsPathByCodec(inpath,outPath,outputCodec)
    val fsOutStream:FSDataOutputStream=fs.create(new Path(secondcompresFile))
    val bufInpStream:BufferedInputStream=new BufferedInputStream(inFSData)
    val inpCompress:CompressionInputStream=codecFirst.createInputStream(bufInpStream)
    val bufOutStream:BufferedOutputStream=new BufferedOutputStream(fsOutStream)
    val outpCompress:CompressionOutputStream=codecSecond.createOutputStream(bufOutStream)
    logger.info(s"compression codec begin   ||  ${secondcompresFile}")
    val start = System.currentTimeMillis()
    try{
      while ({
        readlen=inpCompress.read(ioBuffer)
        readlen != -1
      }){
        outpCompress.write(ioBuffer,0,readlen)
      }
    }catch {
      case e :Exception =>e.printStackTrace()
    }finally {
      outpCompress.flush()
      outpCompress.finish()
      outpCompress.close()
      IOUtils.closeStream(fsOutStream)
      IOUtils.closeStream(inFSData)
    }
    val end = System.currentTimeMillis()
    val timeCause = end - start
    logger.info("compress codec finish  || " + secondcompresFile + "  timecause  " + timeCause + "ms")

  }

}
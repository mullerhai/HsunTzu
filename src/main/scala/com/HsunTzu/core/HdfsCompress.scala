package com.HsunTzu.core

import java.io.{BufferedInputStream, BufferedOutputStream}

import com.HsunTzu.hdfs.HdfsCodec
import com.HsunTzu.utils.CommonUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.io.compress._
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

class HdfsCompress {

}

object HdfsCompress{

  private[this] val logger = Logger(LoggerFactory.getLogger(classOf[HdfsCompress]))

  /**
    * 按目录对文件  进行 snappy  gzip  lzo  压缩
    * @param fs  注意调用方 要主动 做  fs 的 close
    * @param conf
    * @param inpath 输入目录
    * @param outPath 输出目录
    * @param codec  压缩格式 缩写  SNAPPY  GZIP LZO  DEFALTE 默认为defalte
    * @param propertiesPath  压缩文件类型刷选 属性文件路径
    */
  def DirCompressBySnappyGzipLzoCodec(fs: FileSystem, conf: Configuration, inpath: String, outPath: String, codec: String="GZIP")(propertiesPath: String="/usr/local/info.properties"): Unit = {
    val inputPa: Path = new Path(inpath)
    val fsStatus: FileStatus = fs.getFileStatus(inputPa)
    var flag = false
    try{
      if (fsStatus.isFile) {
        logger.info(s"file  inpath  || + ${inpath} ")
        flag = CommonUtils.boolFilePrefixContains(inpath, propertiesPath)
        if (flag) {
          compressHdfsFileSelectCodec(fs,conf,inpath,outPath,codec)
        }
      } else if (fsStatus.isDirectory) {
        val listFs: Array[FileStatus] = fs.listStatus(inputPa)
        if(listFs.length==0){
          return
        }
        listFs.foreach(fil => {
          val fsiN = fil.getPath.getName
          logger.info(s"path dir name   ${fsiN }")
          logger.info(s"path parent ${fil.getPath.getParent }")

          val uriPath = fil.getPath.getParent.toString
          var newInp = ""
          if (uriPath.contains(":9000/")) {
            val uriIndex = uriPath.indexOf(":9000/")
            newInp = uriPath.substring(uriIndex + 5) + "/" + fsiN
          } else {
            newInp = uriPath + "/" + fsiN
          }
          if (fil.isFile) {
            logger.info(s"file 2  inpath  || ${inpath}")

            flag = CommonUtils.boolFilePrefixContains(newInp, propertiesPath)
            if (flag) {
              compressHdfsFileSelectCodec(fs,conf,newInp,outPath,codec)
            }
          } else {
            DirCompressBySnappyGzipLzoCodec(fs, conf, newInp, outPath, codec)(propertiesPath)
          }
        })
      }

    }catch {
      case  e :Exception =>e.printStackTrace()
    }finally {
      // fs.close()
    }

  }


  /***
    * 通用  压缩 格式 选择
    * @param fs
    * @param conf
    * @param inpath
    * @param outPath
    * @param codecMethod
    */
  def compressHdfsFileSelectCodec(fs: FileSystem, conf: Configuration, inpath: String, outPath: String, codecMethod: String):Unit={
    val inputPath: Path = new Path(inpath)
    val inFSData: FSDataInputStream = fs.open(inputPath)
    var readlen = 0
    val ioBuffer: Array[Byte] = new Array[Byte](20 * 1024)
    val codec:CompressionCodec =HdfsCodec.codecStrToCodec(codecMethod)
    HdfsCodec.codecTosetConf(codecMethod,codec,conf)
    val buffInStream: BufferedInputStream = new BufferedInputStream(inFSData)
    val inSubPath: String =CommonUtils.getOutFileSubPath(inpath)
    var nOutPath = ""
    if (outPath.endsWith("/")) {
      nOutPath = outPath.substring(0, outPath.length - 1)
    } else {
      nOutPath = outPath
    }
    val compressFile = nOutPath + inSubPath + codec.getDefaultExtension
    val outPaths: Path = new Path(compressFile)
    val fsOStream: FSDataOutputStream = fs.create(outPaths)
    val bufStream: BufferedOutputStream = new BufferedOutputStream(fsOStream)
    val compressFSoutStream: CompressionOutputStream = codec.createOutputStream(bufStream)
    logger.info("compression codec begin   || " + compressFile)
    val start = System.currentTimeMillis()
    try {
      while ( {
        readlen = buffInStream.read(ioBuffer)
        readlen != -1
      }) {
        compressFSoutStream.write(ioBuffer, 0, readlen)
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      compressFSoutStream.flush()
      compressFSoutStream.finish()
      compressFSoutStream.close()
      IOUtils.closeStream(inFSData)
      IOUtils.closeStream(fsOStream)
      // fs.close()
    }
    val end = System.currentTimeMillis()
    val timeCause = end - start
    logger.info("compress codec finish  || " + compressFile + "  timecause  " + timeCause + "ms")

  }









}
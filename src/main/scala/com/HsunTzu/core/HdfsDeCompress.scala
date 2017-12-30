package com.HsunTzu.core

import java.io.BufferedInputStream

import com.HsunTzu.hdfs.HdfsCodec
import com.HsunTzu.utils.{CommonUtils, HdfsUtils}
import com.typesafe.scalalogging.Logger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.io.compress._
import org.slf4j.LoggerFactory

class HdfsDeCompress {

}

object HdfsDeCompress{


  private[this] val logger=Logger(LoggerFactory.getLogger(classOf[HdfsDeCompress]))

  val HDFSPORT="9000"
  val HDFSPORTDOTSUFFIX=":"+HDFSPORT+"/"
  /**
    * 目录级别 解压缩文件
    * @param fs
    * @param conf
    * @param inpath
    * @param outPath
    * @param codec
    * @param propertiesPath
    */
  def dirCompressFileToOriginFile(fs: FileSystem, conf: Configuration, inpath: String, outPath: String,codec:String)(propertiesPath: String="/usr/local/info.properties"):Unit={
    val inputPa: Path = new Path(inpath)
    val fsStatus: FileStatus = fs.getFileStatus(inputPa)
    var flag = false
    try{
      if(fsStatus.isFile){
        flag =CommonUtils.boolFilePrefixContains(inpath, propertiesPath)
        if (flag) {
          logger.info("one first file || "+inpath+"  codec  "+codec+"  outpath "+outPath)
          normalDeCompressForHdfsFile(fs, conf, inpath, outPath, codec)(propertiesPath)
        }
      }else if (fsStatus.isDirectory) {
        val fslist: Array[FileStatus] = fs.listStatus(inputPa)
        if (fslist.length >=1) {
          logger.info("this Decompress Directory file list count || "+fslist.length)
          fslist.foreach(
            fins => {
              val newInp =HdfsUtils.getNewSubInpathByFileStatus(fins)
              if (fins.isFile) {
                flag = CommonUtils.boolFilePrefixContains(newInp, propertiesPath)
                if (flag) {
                  logger.info("begin decompress newInp "+newInp +"  uriPath  "+ uriPath+" codec "+codec )
                  normalDeCompressForHdfsFile(fs, conf, newInp, outPath, codec)(propertiesPath)
                }
              } else {
                logger.info("begin  loop  next decompress")
                dirCompressFileToOriginFile(fs, conf, newInp, outPath, codec)(propertiesPath)
              }

            })
        }
      }
    }catch {
      case e:Exception => e.printStackTrace()
    }finally {
      // fs.close()
    }


  }

  /**
    * 单文件 解压缩   // fsOutStream.finalize()  fs.close 都不可以使用
    * @param fs
    * @param conf
    * @param inpath
    * @param outPath
    * @param codecSignal
    * @param propertiesPath
    */
  def  normalDeCompressForHdfsFile(fs: FileSystem, conf: Configuration, inpath: String, outPath: String,codecSignal:String)(propertiesPath: String="/usr/local/info.properties"):Unit={
    val inputPath: Path = new Path(inpath)
    val inFSData: FSDataInputStream = fs.open(inputPath)
    var readlen = 0
    val ioBuffer: Array[Byte] = new Array[Byte](64 * 1024)
    val codecClass :CompressionCodec =HdfsCodec.codecStrToCodec(codecSignal)
    HdfsCodec.codecTosetConf(codecSignal,codecClass,conf)
    val DeCompressFile = HdfsUtils.decompressOutFilePath(inpath,outPath)
    logger.info("DeCompressFile  || "+DeCompressFile)
    val outPaths: Path = new Path(DeCompressFile)
    val fsOutStream: FSDataOutputStream = fs.create(outPaths)
    val comInputStream:CompressionInputStream = codecClass.createInputStream(inFSData)
    val  bufInPStream:BufferedInputStream=new BufferedInputStream(comInputStream)
    val start= System.currentTimeMillis()
    logger.info("decompress begin || compress file  "+DeCompressFile)
    try{
      while ({
        readlen=bufInPStream.read(ioBuffer)
        readlen != -1
      }){
        fsOutStream.write(ioBuffer,0,readlen)
      }
    }catch{
      case e:Exception =>e.printStackTrace()
    }finally {
      fsOutStream.flush()
      fsOutStream.close()
      IOUtils.closeStream(fsOutStream)
      IOUtils.closeStream(inFSData)
    }
    val end =System.currentTimeMillis()
    val timeCause= end-start
    logger.info(" decompress finish compressFile  "+DeCompressFile+"  || 时间消耗  "+timeCause+" ms")

  }















  def dirCompressFilesToTarball(fs: FileSystem, conf: Configuration, inpath: String, outPath: String,codec:String)(propertiesPath: String="/usr/local/info.properties"):Unit= {

  }
  def compressFileToTarEntry(fs: FileSystem, conf: Configuration, inpath: String, outPath: String,codec:String)(propertiesPath: String="/usr/local/info.properties"):Unit={
    val inputPath: Path = new Path(inpath)
    val inFSData: FSDataInputStream = fs.open(inputPath)
    val outPaths: Path = new Path(outPath)
    val fsOutStream: FSDataOutputStream = fs.create(outPaths)
    var readlen = 0
    val ioBuffer: Array[Byte] = new Array[Byte](64 * 1024)
    val codecClass :CompressionCodec =HdfsCodec.codecStrToCodec(codec)

    //val tarAchilles：TarArchiveEntry=new TarArchiveEntry()
  }






  def hdfsFileDeCompressBySnappyCodec(fs: FileSystem, conf: Configuration, inpath: String, outPath: String): Unit = {
    //解压缩是 读取 compress流 【fs -buffer- compress],写入 fsData流
    val inputPath: Path = new Path(inpath)
    val inData: FSDataInputStream = fs.open(inputPath)
    val outPaths: Path = new Path(outPath)
    val fsOStream: FSDataOutputStream = fs.create(outPaths)
    var readlen = 0
    val ioBuffer: Array[Byte] = new Array[Byte](64 * 1024)
    val snappyCC: SnappyCodec = new SnappyCodec()
    snappyCC.setConf(conf)
    val bufINStream: BufferedInputStream = new BufferedInputStream(snappyCC.createInputStream(inData))
    try {
      while ( {
        readlen = bufINStream.read(ioBuffer)
        readlen != -1
      }) {
        fsOStream.write(ioBuffer, 0, readlen)
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {

      fsOStream.flush()
      // fsOStream.finalize()
      fsOStream.close()
      IOUtils.closeStream(fsOStream)
      IOUtils.closeStream(inData)
      // fs.close()
    }
  }

  def hdfsFileDeCompressByGzipCodec(fs: FileSystem, conf: Configuration, inpath: String, outPath: String): Unit = {
    val inputPath: Path = new Path(inpath)
    val inFSData: FSDataInputStream = fs.open(inputPath)
    val outPaths: Path = new Path(outPath)
    val fsOStream: FSDataOutputStream = fs.create(outPaths)
    var readlen = 0
    val ioBuffer: Array[Byte] = new Array[Byte](64 * 1024)
    val gcc: GzipCodec = new GzipCodec()
    gcc.setConf(conf)
    val bufINStream: BufferedInputStream = new BufferedInputStream(gcc.createInputStream(inFSData))
    //    val bufOuStream: BufferedOutputStream = new BufferedOutputStream(fsOStream)
    //    val compressOutSream: CompressionOutputStream = gcc.createOutputStream(bufOuStream)
    try {

      while ( {
        readlen = bufINStream.read(ioBuffer)
        readlen != -1
      }) {
        fsOStream.write(ioBuffer, 0, readlen)
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      fsOStream.flush()
      // fsOStream.finalize()
      fsOStream.close()
      IOUtils.closeStream(inFSData)
      IOUtils.closeStream(fsOStream)
      //  fs.close()
    }

  }

  def hdfsFileDeCompressByLZOCodec(fs: FileSystem, conf: Configuration, inpath: String, outPath: String): Unit = {
    val inputPath: Path = new Path(inpath)
    val inFSData: FSDataInputStream = fs.open(inputPath)
    val outPaths: Path = new Path(outPath)
    val fsOStream: FSDataOutputStream = fs.create(outPaths)
    var readlen = 0
    val ioBuffer: Array[Byte] = new Array[Byte](64 * 1024)
    val lzoCC: Lz4Codec = new Lz4Codec()
    lzoCC.setConf(conf)
    val bufINStream: BufferedInputStream = new BufferedInputStream(lzoCC.createInputStream(inFSData))
    // val lzoDeCompressFsInStream: CompressionInputStream = lzoCC.createInputStream(bufINStream)
    try {
      while ( {
        readlen = bufINStream.read(ioBuffer)
        readlen != -1
      }) {
        fsOStream.write(ioBuffer, 0, readlen)
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      fsOStream.flush()
      // fsOStream.finalize()
      fsOStream.close()
      IOUtils.closeStream(inFSData)
      IOUtils.closeStream(fsOStream)
      //fs.close()
    }
  }


}

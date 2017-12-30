package com.HsunTzu.core

import java.io.BufferedInputStream

import com.HsunTzu.hdfs.HdfsCodec
import com.HsunTzu.utils.CommonUtils
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
          logger.info("fslist length || "+fslist.length)
          fslist.foreach(
            fins => {
              val fsiN = fins.getPath.getName
              logger.info( "fsiN Decomposition  "+fsiN)
              val uriPath = fins.getPath.getParent.toString
              var newInp = ""
              if (uriPath.contains(":9000/")) {
                val uriIndex = uriPath.indexOf(":9000/")
                newInp = uriPath.substring(uriIndex + 5) + "/" + fsiN
              } else {
                newInp = uriPath + "/" + fsiN
              }
              logger.info("decompress newInp "+newInp +"  uriPath  "+ uriPath )
              if (fins.isFile) {
                flag = CommonUtils.boolFilePrefixContains(newInp, propertiesPath)
                if (flag) {
                  logger.info("begin  normal  decompress  one ")
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

  def  normalDeCompressForHdfsFile(fs: FileSystem, conf: Configuration, inpath: String, outPath: String,codecSignal:String)(propertiesPath: String="/usr/local/info.properties"):Unit={
    val inputPath: Path = new Path(inpath)
    val inFSData: FSDataInputStream = fs.open(inputPath)
    var readlen = 0
    val ioBuffer: Array[Byte] = new Array[Byte](64 * 1024)
    val codecClass :CompressionCodec =HdfsCodec.codecStrToCodec(codecSignal)
    HdfsCodec.codecTosetConf(codecSignal,codecClass,conf)
    val inSubPathExtension: String =CommonUtils.getOutFileSubPath(inpath)
    val inSubPath =inSubPathExtension.substring(0,inSubPathExtension.lastIndexOf("."))
    var nOutPath = ""
    if (outPath.endsWith("/")) {
      nOutPath = outPath.substring(0, outPath.length - 1)
    } else {
      nOutPath = outPath
    }
    val DeCompressFile = nOutPath + inSubPath
    // + codec.getDefaultExtension
    logger.info("DeCompressFile  || "+DeCompressFile)
    val outPaths: Path = new Path(DeCompressFile)
    val fsOutStream: FSDataOutputStream = fs.create(outPaths)
    //    val defalteCodec:DeflateCodec=new DeflateCodec()
    //    defalteCodec.setConf(conf)
    //val comInputStream:CompressionInputStream =defalteCodec.createInputStream(inFSData)
    val comInputStream:CompressionInputStream = codecClass.createInputStream(inFSData)
    val  bufInPStream:BufferedInputStream=new BufferedInputStream(comInputStream)
    //val bufOutStream:BufferedOutputStream=new BufferedOutputStream(fsOutStream)

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
      // fsOutStream.finalize()
      fsOutStream.close()
      IOUtils.closeStream(fsOutStream)
      IOUtils.closeStream(inFSData)
    }
    val end =System.currentTimeMillis()
    val timeCause= end-start
    logger.info(" decompress finish compressFile  "+DeCompressFile+"  || 时间消耗  "+timeCause+" ms")

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

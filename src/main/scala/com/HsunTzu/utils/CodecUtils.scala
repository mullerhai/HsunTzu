package com.HsunTzu.utils

import java.io.{BufferedInputStream, BufferedOutputStream}

import com.typesafe.scalalogging.Logger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.io.compress._
import org.slf4j.LoggerFactory

class CodecUtils {

}
object  CodecUtils{


  private [this] val logger=Logger(LoggerFactory.getLogger(classOf[CodecUtils]))
  /** *
    * Hdfs 文件  Gzip 压缩
    *
    * @param fs
    * @param conf
    * @param inpath
    * @param outPath
    */
  def hdfsFileCompressByGzipCodec(fs: FileSystem, conf: Configuration, inpath: String, outPath: String): Unit = {
    val inputPath: Path = new Path(inpath)
    val inFSData: FSDataInputStream = fs.open(inputPath)
    var readlen = 0
    val ioBuffer: Array[Byte] = new Array[Byte](20 * 1024)
    val gcc: GzipCodec = new GzipCodec()
    gcc.setConf(conf)
    val buffInStream: BufferedInputStream = new BufferedInputStream(inFSData)
    val inSubPath: String =CommonUtils.getOutFileSubPath(inpath)
    var nOutPath = ""
    if (outPath.endsWith("/")) {
      nOutPath = outPath.substring(0, outPath.length - 1)
    } else {
      nOutPath = outPath
    }
    val gzipFile = nOutPath + inSubPath + gcc.getDefaultExtension
    val outPaths: Path = new Path(gzipFile)
    val fsOStream: FSDataOutputStream = fs.create(outPaths)
    val bufStream: BufferedOutputStream = new BufferedOutputStream(fsOStream)
    val gzipFSoutStream: CompressionOutputStream = gcc.createOutputStream(bufStream)
    logger.info("gzip codec begin   || " + gzipFile)
    val start = System.currentTimeMillis()
    try {
      while ( {
        readlen = buffInStream.read(ioBuffer)
        readlen != -1
      }) {
        gzipFSoutStream.write(ioBuffer, 0, readlen)
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      gzipFSoutStream.flush()
      gzipFSoutStream.finish()
      gzipFSoutStream.close()
      IOUtils.closeStream(inFSData)
      IOUtils.closeStream(fsOStream)
      // fs.close()
    }
    val end = System.currentTimeMillis()
    val timeCause = end - start
    logger.info("gzip codec finish  || " + gzipFile + "  timecause  " + timeCause + "ms")

  }

  /** *
    * hdfs 文件 Lzo 格式压缩
    *
    * @param fs
    * @param conf
    * @param inpath
    * @param outPath
    */
  def LZOCodecHdfsFileCompressBy(fs: FileSystem, conf: Configuration, inpath: String, outPath: String): Unit = {
    val inputPath: Path = new Path(inpath)
    val inFSData: FSDataInputStream = fs.open(inputPath)
    val ioBuffer: Array[Byte] = new Array[Byte](64 * 1024)
    val bufINStream: BufferedInputStream = new BufferedInputStream(inFSData)
    val lzoCC: Lz4Codec = new Lz4Codec()
    lzoCC.setConf(conf)
    val inSubPath: String =CommonUtils.getOutFileSubPath(inpath)
    var nOutPath = ""
    if (outPath.endsWith("/")) {
      nOutPath = outPath.substring(0, outPath.length - 1)
    } else {
      nOutPath = outPath
    }
    val LzoFile = nOutPath + inSubPath + lzoCC.getDefaultExtension
    val outPaths: Path = new Path(LzoFile)
    val fsOStream: FSDataOutputStream = fs.create(outPaths)
    var readlen = 0
    val buffOutStream: BufferedOutputStream = new BufferedOutputStream(fsOStream)
    val lzoCompressFsOutStream: CompressionOutputStream = lzoCC.createOutputStream(buffOutStream)
    val start = System.currentTimeMillis()
    logger.info("lzo codec begining  ||  " + LzoFile)
    try {
      while ( {
        readlen = bufINStream.read(ioBuffer)
        readlen != -1
      }) {
        lzoCompressFsOutStream.write(ioBuffer, 0, readlen)
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      lzoCompressFsOutStream.flush()
      lzoCompressFsOutStream.finish()
      lzoCompressFsOutStream.close()
      IOUtils.closeStream(inFSData)
      IOUtils.closeStream(fsOStream)
      //fs.close()
    }
    val end = System.currentTimeMillis()
    val timecause = end - start
    logger.info("lzo codec finish || LzoFile  " + LzoFile + "  time cause || " + timecause + " ms")

  }

  def deflateCompressForHdfsFile(fs: FileSystem, conf: Configuration, inpath: String, outPath: String):Unit={
    val inputPath: Path = new Path(inpath)
    val inFsData: FSDataInputStream = fs.open(inputPath)
    val deflateCodec:DeflateCodec=new DeflateCodec()
    deflateCodec.setConf(conf)
    val bufInStream:BufferedInputStream=new BufferedInputStream(inFsData)
    val inSubPath: String =CommonUtils.getOutFileSubPath(inpath)
    var nOutPath = ""
    if (outPath.endsWith("/")) {
      nOutPath = outPath.substring(0, outPath.length - 1)
    } else {
      nOutPath = outPath
    }
    val defalteFile=nOutPath + inSubPath +deflateCodec.getDefaultExtension
    val outdir:Path=new Path(defalteFile)
    val fsDataOutStream: FSDataOutputStream = fs.create(outdir)
    val fsBufferOutStream: BufferedOutputStream = new BufferedOutputStream(fsDataOutStream)
    val compressOutStream: CompressionOutputStream=deflateCodec.createOutputStream(fsBufferOutStream)
    val ioBuffer: Array[Byte] = new Array[Byte](64 * 1024)
    var readLen: Int = 0
    val start = System.currentTimeMillis()
    logger.info("deflate  codec begining  ||  " + defalteFile)
    try {
      while ( {
        readLen = bufInStream.read(ioBuffer)
        readLen != -1
      }) {
        compressOutStream.write(ioBuffer, 0, readLen)
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      compressOutStream.flush()
      compressOutStream.finish()
      compressOutStream.close()
      IOUtils.closeStream(inFsData)
      IOUtils.closeStream(fsDataOutStream)
      //fs.close()
    }
    val end = System.currentTimeMillis()
    val timeCause = end - start
    logger.info("defalte  codec finish   ||  " + defalteFile + " || 时间消耗   " + timeCause + " ms")


  }
  /**
    * hdfs 文件  snappy 压缩
    *
    * @param fs
    * @param conf
    * @param inpath
    * @param outPath
    */
  def hdfsFileCompressBySnappyCodec(fs: FileSystem, conf: Configuration, inpath: String, outPath: String): Unit = {
    //压缩时 读取fsdata流  写入 compress流【fs-buff-compress]
    val inputPath: Path = new Path(inpath)
    val inFsData: FSDataInputStream = fs.open(inputPath)
    val snappyCC: SnappyCodec = new SnappyCodec()
    snappyCC.setConf(conf)
    val inSubPath: String = CommonUtils.getOutFileSubPath(inpath)
    var nOutPath = ""
    if (outPath.endsWith("/")) {
      nOutPath = outPath.substring(0, outPath.length - 1)
    } else {
      nOutPath = outPath
    }
    val snappyFile: String = nOutPath + inSubPath + snappyCC.getDefaultExtension
    val outdir: Path = new Path(snappyFile)
    val fsDataOutStream: FSDataOutputStream = fs.create(outdir)
    val fsBufferOutStream: BufferedOutputStream = new BufferedOutputStream(fsDataOutStream)
    val compressOutStream: CompressionOutputStream = snappyCC.createOutputStream(fsBufferOutStream)
    val bufInpStream: BufferedInputStream = new BufferedInputStream(inFsData)
    val ioBuffer: Array[Byte] = new Array[Byte](64 * 1024)
    var readLen: Int = 0
    val start = System.currentTimeMillis()
    logger.info("snappy codec begining  ||  " + snappyFile)
    try {
      while ( {
        readLen = bufInpStream.read(ioBuffer)
        readLen != -1
      }) {
        compressOutStream.write(ioBuffer, 0, readLen)
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      compressOutStream.flush()
      compressOutStream.finish()
      compressOutStream.close()
      IOUtils.closeStream(inFsData)
      IOUtils.closeStream(fsDataOutStream)
      //fs.close()
    }
    val end = System.currentTimeMillis()
    val timeCause = end - start
    logger.info("snappy codec finish   ||  " + snappyFile + " || 时间消耗   " + timeCause + " ms")

  }

}

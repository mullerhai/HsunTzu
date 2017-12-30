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

 //private[this] val logger = Logger(LoggerFactory.getILogger("Core.HdfsCompress"))

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
          codec match {
            case "SNAPPY" => hdfsFileCompressBySnappyCodec(fs, conf, inpath, outPath)
            case "GZIP" => hdfsFileCompressByGzipCodec(fs, conf, inpath, outPath)
            case "LZO" => LZOCodecHdfsFileCompressBy(fs, conf, inpath, outPath)
            case "DEFALTE" =>deflateCompressForHdfsFile(fs,conf,inpath,outPath)
            case _ => deflateCompressForHdfsFile(fs,conf,inpath,outPath)
          }
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
              codec match {
                case "SNAPPY" => hdfsFileCompressBySnappyCodec(fs, conf, newInp, outPath)
                case "GZIP" => hdfsFileCompressByGzipCodec(fs, conf, newInp, outPath)
                case "LZO" => LZOCodecHdfsFileCompressBy(fs, conf, newInp, outPath)
                case "DEFALTE" =>deflateCompressForHdfsFile(fs,conf,newInp,outPath)
                case _ => deflateCompressForHdfsFile(fs,conf,newInp,outPath)
              }
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
          val fsiN = fins.getPath.getName
          val uriPath = fins.getPath.getParent.toString
          var newInp = ""
          if (uriPath.contains(":9000/")) {
            val uriIndex = uriPath.indexOf(":9000/")
            newInp = uriPath.substring(uriIndex + 5) + "/" + fsiN
          } else {
            newInp = uriPath + "/" + fsiN
          }
          if (fins.isFile) {
            logger.info(s"new inp  ${newInp}  uriPath  ${uriPath}  fsin ${fsiN}")
            flag = CommonUtils.boolFilePrefixContains(newInp, propertiesPath)
            if (flag) {
              logger.info(s"outPath ${outPath}  input Codec ${inputCodec}  outputCodec  ${outputCodec}")
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
    val inSubPathExtension: String =CommonUtils.getOutFileSubPath(inpath)
    val inSubPath =inSubPathExtension.substring(0,inSubPathExtension.lastIndexOf("."))
    var nOutPath = ""
    if (outPath.endsWith("/")) {
      nOutPath = outPath.substring(0, outPath.length - 1)
    } else {
      nOutPath = outPath
    }
    val secondcompresFile=nOutPath+inSubPath+codecSecond.getDefaultExtension
    println("one "+inputPath+"secondcompresFile || "+secondcompresFile + "  outpath "+nOutPath+" inSubPath  "+inSubPath+" ex "+codecSecond.getDefaultExtension)
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

  def DirHdfsDeCompressByNormalCodec(fs: FileSystem, conf: Configuration, inpath: String, outPath: String, codecMethod: String): Unit = {
    val inputPa: Path = new Path(inpath)
    val fsStatus: FileStatus = fs.getFileStatus(inputPa)
    val cm = codecMethod match {
      case "SNAPPY" => hdfsFileCompressBySnappyCodec(fs, conf, inpath, outPath)
      case "GZIP" => hdfsFileCompressByGzipCodec(fs, conf, inpath, outPath)
      case "LZO" => LZOCodecHdfsFileCompressBy(fs, conf, inpath, outPath)
      case _ => hdfsFileCompressByNormalCodec(fs, conf, inpath, outPath, "")
    }

    if (fsStatus.isFile) {

      hdfsFileDeCompressByNormalCodec(fs, conf, inpath, outPath, codecMethod)
    } else if (fsStatus.isDirectory) {
      val listFs: Array[FileStatus] = fs.listStatus(inputPa)
      listFs.foreach(fil => {
        val fsiN = fil.getPath.toString
        hdfsFileDeCompressByNormalCodec(fs, conf, fsiN, outPath, codecMethod)
      })
    }

  }

  def DirHdfsCompressByNormalCodec(fs: FileSystem, conf: Configuration, inpath: String, outPath: String, codec: String): Unit = {
    val inputPath: Path = new Path(inpath)
    val fsStatus: FileStatus = fs.getFileStatus(inputPath)
    if (fsStatus.isFile) {
      hdfsFileCompressByNormalCodec(fs, conf, inpath, outPath, codec)
    } else if (fsStatus.isDirectory) {
      val listFils: Array[FileStatus] = fs.listStatus(inputPath)
      listFils.foreach(fi => {
        val fsIn = fi.getPath.toString
        hdfsFileCompressByNormalCodec(fs, conf, fsIn, outPath, codec)
      })

    }
  }

  def hdfsFileDeCompressByNormalCodec(fs: FileSystem, conf: Configuration, inpath: String, outPath: String, codec: String): Unit = {
    val inputPath: Path = new Path(inpath)
    val inFSData: FSDataInputStream = fs.open(inputPath)

    val outPaths: Path = new Path(outPath)
    val fsOStream: FSDataOutputStream = fs.create(outPaths)
    var readlen = 0
    val ioBuffer: Array[Byte] = new Array[Byte](64 * 1024)
    val bufINStream: BufferedInputStream = new BufferedInputStream(inFSData)
    val comFactor = new CompressionCodecFactory(conf)
    val normalCodec = comFactor.getCodecByClassName("")
    val compressInStream: CompressionInputStream = normalCodec.createInputStream(bufINStream)
    try {
      while ( {
        readlen = compressInStream.read(ioBuffer)
        readlen != -1
      }) {
        fsOStream.write(ioBuffer, 0, readlen)
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      IOUtils.closeStream(inFSData)
      IOUtils.closeStream(fsOStream)
      // fs.close()
    }

  }

  def hdfsFileCompressByNormalCodec(fs: FileSystem, conf: Configuration, inpath: String, outPath: String, codec: String): Unit = {
    val inputPath: Path = new Path(inpath)
    val inFSData: FSDataInputStream = fs.open(inputPath)
    val outPaths: Path = new Path(outPath)
    val fsOStream: FSDataOutputStream = fs.create(outPaths)
    var readlen = 0
    val ioBuffer: Array[Byte] = new Array[Byte](64 * 1024)
    val bufINStream: BufferedInputStream = new BufferedInputStream(inFSData)
    val bufOUtStream: BufferedOutputStream = new BufferedOutputStream(fsOStream)
    val comFactor = new CompressionCodecFactory(conf)
    val cmCodec = comFactor.getCodecByClassName(codec)
    val compreOutStream: CompressionOutputStream = cmCodec.createOutputStream(bufOUtStream)
    try {
      while ( {
        readlen = bufINStream.read(ioBuffer)
        readlen != -1
      }) {
        compreOutStream.write(ioBuffer, 0, readlen)
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      IOUtils.closeStream(inFSData)
      IOUtils.closeStream(fsOStream)
      // fs.close()
    }


  }




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
    //val snappyComp:SnappyCompressor=new SnappyCompressor()
    snappyCC.setConf(conf)

    // val snappyFile :String= getFileOriginName(inpath) +snappyCC.getDefaultExtension
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
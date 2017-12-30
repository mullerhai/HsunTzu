package com.HsunTzu.core

import java.io.{BufferedInputStream, BufferedOutputStream}

import com.HsunTzu.hdfs.HdfsCodec
import com.HsunTzu.utils.{CommonUtils, HdfsUtils}
import com.typesafe.scalalogging.Logger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.io.compress._
import org.slf4j.LoggerFactory

class HdfsCompress {

}

object HdfsCompress {

  private[this] val logger = Logger(LoggerFactory.getLogger(classOf[HdfsCompress]))

  val HDFSPORT="9000"
  val HDFSPORTDOTSUFFIX=":"+HDFSPORT+"/"

  /**
    * 按目录对文件  进行 snappy  gzip  lzo  压缩
    *
    * @param fs             注意调用方 要主动 做  fs 的 close
    * @param conf
    * @param inpath         输入目录
    * @param outPath        输出目录
    * @param codec          压缩格式 缩写  SNAPPY  GZIP LZO  DEFALTE 默认为defalte
    * @param propertiesPath 压缩文件类型刷选 属性文件路径
    */
  def DirCompressByHDFSCodec(fs: FileSystem, conf: Configuration, inpath: String, outPath: String, codec: String = "GZIP")(propertiesPath: String = "/usr/local/info.properties"): Unit = {
    val inputPa: Path = new Path(inpath)
    val fsStatus: FileStatus = fs.getFileStatus(inputPa)
    var flag = false
    try {
      if (fsStatus.isFile) {
        logger.info(s"single file 1  fileinpath  || ${inpath} outpath  ${outPath} codec ${codec}")
        flag = CommonUtils.boolFilePrefixContains(inpath, propertiesPath)
        if (flag) {
          compressHdfsFileByHDFSSelectCodec(fs, conf, inpath, outPath, codec)
        }
      } else if (fsStatus.isDirectory) {
        val listFs: Array[FileStatus] = fs.listStatus(inputPa)
        if (listFs.length > 0) {
          listFs.foreach(files => {
            val newSubInpath=HdfsUtils.getNewSubInpathByFileStatus(files)
            if (files.isFile) {
              flag = CommonUtils.boolFilePrefixContains(newSubInpath, propertiesPath)
              if (flag) {
                logger.info(s"Directory  subfileinpath  || ${newSubInpath} outpath  ${outPath} codec ${codec}")
                compressHdfsFileByHDFSSelectCodec(fs, conf, newSubInpath, outPath, codec)
              }
            } else {
              DirCompressByHDFSCodec(fs, conf, newSubInpath, outPath, codec)(propertiesPath)
            }
          })
        }
      }

    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      // fs.close()
    }

  }


  /** *
    * 通用  压缩 格式 选择
    *
    * @param fs
    * @param conf
    * @param inpath
    * @param outPath
    * @param codecMethod
    */
  def compressHdfsFileByHDFSSelectCodec(fs: FileSystem, conf: Configuration, inpath: String, outPath: String, codecMethod: String): Unit = {
    val inputPath: Path = new Path(inpath)
    val inFSData: FSDataInputStream = fs.open(inputPath)
    var readlen = 0
    val ioBuffer: Array[Byte] = new Array[Byte](60 * 1024)
    val codec: CompressionCodec = HdfsCodec.codecStrToCodec(codecMethod)
    HdfsCodec.codecTosetConf(codecMethod, codec, conf)
    val buffInStream: BufferedInputStream = new BufferedInputStream(inFSData)
    val compressFile =HdfsUtils.getFileOutHDFSpathByCodec(inpath,outPath,codecMethod)
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
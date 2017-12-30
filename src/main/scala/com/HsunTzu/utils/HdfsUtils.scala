package com.HsunTzu.utils

import com.HsunTzu.hdfs.HdfsCodec
import com.typesafe.scalalogging.Logger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.mapreduce.Job
import org.slf4j.LoggerFactory

class HdfsUtils {

}
object  HdfsUtils{

  private [this] val logger =Logger(LoggerFactory.getLogger(classOf[HdfsUtils]))
  val HDFSPORTDOTSUFFIX=PropertiesUtils.configFileByKeyGetValueFrom("HDFSPORTDOTSUFFIX")

  /**
    * 解压缩  的输出路径 输入为压缩文件 ，输出为原始文件  没有压缩格式后缀
    * @param inpath
    * @param outPath
    * @return
    */
  def decompressOutFilePath(inpath:String,outPath:String):String={
    val inSubPathExtension: String =CommonUtils.getOutFileSubPath(inpath)
    val inSubPath =inSubPathExtension.substring(0,inSubPathExtension.lastIndexOf("."))
    var nOutPath = ""
    if (outPath.endsWith("/")) {
      nOutPath = outPath.substring(0, outPath.length - 1)
    } else {
      nOutPath = outPath
    }
    val DeCompressFile = nOutPath + inSubPath
    return DeCompressFile
  }
  /**
    * 压缩文件 转其他压缩格式 专用 输入文件为压缩文件 输出文件为 压缩文件
    * @param inpath
    * @param outPath
    * @param codecSecStr
    * @return
    */
  def dropExtensionGetOutHdfsPathByCodec(inpath:String,outPath:String,codecFirStr:String,codecSecStr:String):String={
    val inSubPathExtension: String =CommonUtils.getOutFileSubPath(inpath)
    val inSubPath =inSubPathExtension.substring(0,inSubPathExtension.lastIndexOf("."))
    var nOutPath = ""
    if (outPath.endsWith("/")) {
      nOutPath = outPath.substring(0, outPath.length - 1)
    } else {
      nOutPath = outPath
    }
    val codecFirst=HdfsCodec.codecStrToCodec(codecFirStr)
    val codecSecond=HdfsCodec.codecStrToCodec(codecSecStr)
    val flag= HdfsCodec.boolCompresfileExtension(inpath,codecFirst)
    val secondcompresFile=if(flag) nOutPath+inSubPath+codecSecond.getDefaultExtension  else ""
    logger.info("secondcompresFile || "+secondcompresFile + "  outpath "+nOutPath+" inSubPath  "+inSubPath+" ex "+codecSecond.getDefaultExtension)
    return secondcompresFile
  }
  /**
    * 根据输入路径 和输出路径 获取新的输出路径,输入为原始文件 输出文件为 压缩文件
    * @param inpath
    * @param outPath
    * @param codec
    * @return
    */
  def getFileOutHDFSpathByCodec(inpath:String,outPath:String,codec:String):String={
    val inSubPath: String = CommonUtils.getOutFileSubPath(inpath)
    var nOutPath = ""
    if (outPath.endsWith("/")) {
      nOutPath = outPath.substring(0, outPath.length - 1)
    } else {
      nOutPath = outPath
    }
    val codecClass=HdfsCodec.codecStrToCodec(codec)
    logger.info(s"input path ${nOutPath}  outputfile  codec ${codec}")
    val compressFile = nOutPath + inSubPath + codecClass.getDefaultExtension
    return  compressFile
  }

  /**
    * 在目录级别操作 压缩时  根据 fileStatus 获取 新的 子文件 输入路径
    * @param files
    * @return
    */
  def getNewSubInpathByFileStatus(files:FileStatus):String={
   val subFileName=files.getPath.getName
   logger.info(s"path dir name   ${subFileName}")
   logger.info(s"path parent ${files.getPath.getParent}")
   val hdfsUriPath = files.getPath.getParent.toString
   var newSubInpath = ""
   if (hdfsUriPath.contains(HDFSPORTDOTSUFFIX)) {
     val uriIndex = hdfsUriPath.indexOf(HDFSPORTDOTSUFFIX)
     newSubInpath = hdfsUriPath.substring(uriIndex + 5) + "/" + subFileName
   } else {
     newSubInpath = hdfsUriPath + "/" + subFileName
   }
   return newSubInpath
 }
  /**mapreduce 运行成功后 得到 成功后的输出文件
    *
    * @param conf
    * @param job
    * @param outputPath
    * @param successFilesubfix
    * @param distLocalFilePath
    */
  def getSuccefulOutFile(conf: Configuration, job: Job, outputPath: String, successFilesubfix: String = "/part-r-00000", distLocalFilePath: String = "/hadoopJars"): Unit = {

    if (job.waitForCompletion(true)) {
      if (job.isSuccessful) {
        val fs = FileSystem.get(conf)
        val srcHdfsFile = outputPath + successFilesubfix
        val srcPath = new Path(srcHdfsFile)
        val job_id: String = job.getJobID.toString
        val distPath = new Path(distLocalFilePath + "/" + job.getJobName.toString + "/" + job_id)
        if (fs.exists(srcPath)) {
          fs.copyToLocalFile(srcPath, distPath)
        }
      }
    }

  }

  /**
    * 检查hdfs 是否有同名的输出文件目录，若有则删除
    * @param conf
    * @param pathStr
    */
  def checkHdfsOuputExist(conf: Configuration, pathStr: String): Unit = {
    val fs = FileSystem.get(conf)
    val path = new Path(pathStr)
    if (fs.exists(path)) {
      try {
        fs.delete(new Path(pathStr), true)
      }catch {
        case e:Exception => e.printStackTrace()
      }

      logger.info("delete the ouput path successfully")
    }
  }

}

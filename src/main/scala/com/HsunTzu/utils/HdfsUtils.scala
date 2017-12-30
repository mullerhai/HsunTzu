package com.HsunTzu.utils

import com.typesafe.scalalogging.Logger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.Job
import org.slf4j.LoggerFactory

class HdfsUtils {

}
object  HdfsUtils{

  private [this] val logger =Logger(LoggerFactory.getLogger(classOf[HdfsUtils]))
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
  def checkOuputExist(conf: Configuration, pathStr: String): Unit = {
    val fs = FileSystem.get(conf)
    val path = new Path(pathStr)
    if (fs.exists(path)) {
      try {
        fs.delete(new Path(pathStr), true)
      }

      logger.info("delete the ouput path successfully")
    }
  }

}

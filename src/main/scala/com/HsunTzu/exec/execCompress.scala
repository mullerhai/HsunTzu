package com.HsunTzu.exec

import com.HsunTzu.core.{HdfsCompress, HdfsConvertCompress, HdfsDeCompress, HdfsUntar}
import com.HsunTzu.hdfs.HdfsCodec
import com.typesafe.scalalogging.Logger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.compress._
import org.platanios.tensorflow.api.tf
import org.platanios.tensorflow.jni.{Graph, Session}
import org.slf4j.LoggerFactory

/***
  * 通过一个主类设置  选择最后要执行的方法
  */
object  execCompress {

  private [this] val logger =Logger(LoggerFactory.getLogger(classOf[execCompress]))

  /**
    * * 1.输入目录 2.输出目录 3.压缩方法类型  4.属性文件路径 5.输入压缩格式信号量 6输出压缩格式信号量
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {

    val FS = "hdfs://192.168.255.161:9000"
    val FSKey = "fs.defaultFS"
    val FSUser = "HADOOP_USER_NAME"
    val hadoopUser = "linkedme_hadoop"
    System.setProperty(FSUser, hadoopUser)
    val conf: Configuration = new Configuration()
    conf.set(FSKey, FS)
    conf.set(FSUser, hadoopUser)
    val fs = FileSystem.get(conf)

    val inputPath: String = args(0)
    val outdir: String = args(1)
    val compressType: String = args(2)
    val propertiesPath = args(3)
    val inputCodecSignal: String = args(4)
    val outputCodecSignal: String = args(5)
    val exec = new execCompress
    val programStart = System.currentTimeMillis()
    compressType match {
      case "1" => exec.originFileToCompressFile(fs, conf, inputPath, outdir, inputCodecSignal)(propertiesPath)
      case "2" => exec.tarFileToOriginFile(fs, inputPath, outdir, inputCodecSignal)(propertiesPath)
      case "3" => exec.compressFileToOriginFile(fs, conf, inputPath, outdir, inputCodecSignal)(propertiesPath)
      case "4" => exec.oneCompressConvertOtherCompress(fs, conf, inputPath, outdir, inputCodecSignal, outputCodecSignal)(propertiesPath)

      case "5" => exec.originFilesToTarBall(fs, conf, inputPath, outdir, inputCodecSignal)(propertiesPath)
      case "6" => exec.compressFilesToTarball(fs, conf, inputPath, outdir, inputCodecSignal)(propertiesPath)
      case "7" => exec.tarFileToSingleCompressFiles(fs, inputPath, outdir, inputCodecSignal, outputCodecSignal)(propertiesPath)
      case _ => exec.originFileToCompressFile(fs, conf, inputPath, outdir, inputCodecSignal)(propertiesPath)
    }
    val  programEnd =System.currentTimeMillis()
    logger.info("Time  cause "+(programEnd-programStart))
  }

}

class execCompress {

  /**
    * 原始日志 转换为 压缩文件
    *
    * @param fs
    * @param conf
    * @param inpath
    * @param outPath
    * @param codeSignal
    * @param propertiesPath
    */
  def originFileToCompressFile(fs: FileSystem, conf: Configuration, inpath: String, outPath: String, codeSignal: String = "0")(propertiesPath: String = "/usr/local/info.properties"): Unit = {
    val codec: String = HdfsCodec.codecSignalToCodec(codeSignal)
    //可以使用 未来需要重构
    HdfsCompress.DirCompressByHDFSCodec(fs, conf, inpath, outPath, codec)(propertiesPath)

  }

  /**
    * tar包  文件转换为  原始文件
    *
    * @param fs
    * @param inpath
    * @param outPath
    * @param codeSignal
    * @param propertiesPath
    */
  def tarFileToOriginFile(fs: FileSystem, inpath: String, outPath: String, codeSignal: String = "0")(propertiesPath: String = "/usr/local/info.properties"): Unit = {

    //可以使用 未来可以重构
    HdfsUntar.unCompressTarParentDir(inpath, outPath, fs, propertiesPath)
  }

  /** *
    * 压缩文件 转换为 原始文件
    *
    * @param fs
    * @param conf
    * @param inpath
    * @param outPath
    * @param codeSignal
    * @param propertiesPath
    */
  def compressFileToOriginFile(fs: FileSystem, conf: Configuration, inpath: String, outPath: String, codeSignal: String = "0")(propertiesPath: String = "/usr/local/info.properties"): Unit = {

    val codec: String = HdfsCodec.codecSignalToCodec(codeSignal)
    //可以使用 未来需要重构
    HdfsDeCompress.dirCompressFileToOriginFile(fs, conf, inpath, outPath, codec)(propertiesPath)
  }

  /**
    * 一种压缩格式文件  转为另一种 压缩文件
    *
    * @param fs
    * @param conf
    * @param inpath
    * @param outPath
    * @param inputCodecSignal
    * @param outputCodecSignal
    * @param propertiesPath
    */
  def oneCompressConvertOtherCompress(fs: FileSystem, conf: Configuration, inpath: String, outPath: String, inputCodecSignal: String, outputCodecSignal: String)(propertiesPath: String = "/usr/local/info.properties"): Unit = {

    val inputCodec: String = HdfsCodec.codecSignalToCodec(inputCodecSignal)
    val outputCodec: String = HdfsCodec.codecSignalToCodec(outputCodecSignal)
    if (inputCodec.trim != outputCodec.trim) {
      HdfsConvertCompress.dirConverterOneCompressToOtherCompress(fs, conf, inpath, outPath, inputCodec, outputCodec)(propertiesPath)
    }
  }


  def tarFileToSingleCompressFiles(fs: FileSystem, inpath: String, outPath: String, inputCodec: String, outCodec: String)(propertiesPath: String = "/usr/local/info.properties"): Unit = {
    //    hdfsUntar.dirTarfileToSingleCompressFiles(fs, inpath, outPath, inputCodec, outCodec)(propertiesPath)

  }

  def compressFilesToTarball(fs: FileSystem, conf: Configuration, inpath: String, outPath: String, codeSignal: String = "0")(propertiesPath: String = "/usr/local/info.properties"): Unit = {

    HdfsDeCompress.dirCompressFilesToTarball(fs, conf, inpath, outPath, codeSignal)(propertiesPath)
  }

  def originFilesToTarBall(fs: FileSystem, conf: Configuration, inpath: String, outPath: String, codeSignal: String = "0")(propertiesPath: String = "/usr/local/info.properties"): Unit = {

  }

}



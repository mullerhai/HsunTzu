package com.HsunTzu.core

import java.io.InputStream
import java.util.zip.{ZipEntry, ZipInputStream}

import com.HsunTzu.utils.{CommonUtils, FileUtils, PropertiesUtils}
import com.typesafe.scalalogging.Logger
import org.apache.hadoop.fs.{FSDataOutputStream, Path, FileSystem => HDFSFileSystem}
import org.apache.hadoop.io.IOUtils
import org.apache.tools.tar.{TarEntry, TarInputStream}
import org.slf4j.LoggerFactory

import scala.util.control.Breaks

class HdfsUntar {

}

object  HdfsUntar{


  private [this] val logger=Logger(LoggerFactory.getLogger(classOf[HdfsUntar]))

  val HDFSPORTDOTSUFFIX=PropertiesUtils.configFileByKeyGetValueFrom("HDFSPORTDOTSUFFIX")

  val  targz=".tar.gz"
  val tarbz2 =".tar.bz2"
  val  tgz=".tgz"
  val  tar =".tar"

  val  zip =".zip"
  val  gz =".gz"
  val  bz2=".bz2"
  /**
    * 解压 hdfs tar 文件父级文件夹
    * @param srcDir
    * @param outputDir
    * @param fs
    * @param propertiesPath
    */
  def  unCompressTarParentDir(srcDir: String, outputDir: String, fs: HDFSFileSystem,propertiesPath:String): Unit = {
    if(boolTarFileSuffix(srcDir)){
      logger.info("single tar file outputDir "+outputDir)
      newUnCompressFile(srcDir,outputDir,fs,propertiesPath)
    }else{
      val hdfsTarfiles=fs.listFiles(new Path(srcDir),true)
      while (hdfsTarfiles.hasNext){
        val tarName=hdfsTarfiles.next().getPath.toString
        if(boolTarFileSuffix(tarName)){
          logger.info("parent tarname  "+tarName +" output dir "+outputDir)
          newUnCompressFile(tarName,outputDir,fs,propertiesPath)
        }
      }
    }
  }

  /**
    * 解压 单个tar 文件
    * @param srcDir
    * @param outputDir
    * @param fs
    * @param propertiesPath
    */
  def newUnCompressFile(srcDir: String, outputDir: String, fs: HDFSFileSystem,propertiesPath:String): Unit = {
    val srcTemp: String = srcDir.toLowerCase()
    if(boolTarFileSuffix(srcTemp)){
      tarFileUnCompress(srcDir,outputDir,fs,propertiesPath)
    }else if(boolCompressFileSuffix(srcTemp)){
      zipBz2gzipFileUnCompress(srcDir,outputDir,fs)
    }
  }


  /**
    * tar 包类型压缩文件解压
    *
    * @param srcDir
    * @param outputDir
    * @param propertiesPath
    * @param fs
    */
  def tarFileUnCompress(srcDir: String, outputDir: String, fs: HDFSFileSystem, propertiesPath: String): Unit = {
    val tarln: TarInputStream =FileUtils.tarfileStreamBySuffix(srcDir,fs)
    logger.info("tarFileUnCompress uncompressing" + fs.getUri + srcDir + "fs" + fs)
    var entry: TarEntry = null
    while ( {
      entry = tarln.getNextEntry; entry != null
    }) {
      try {
        if (entry.isDirectory) {
          val dirPath=outputDir + "/" + entry.getName
          logger.info("tar entry.getName  " + entry.getName + "   || outputDir:  " + outputDir+"dirPath"+dirPath)
          val ps: Path = new Path(dirPath)
          val mkflag=fs.mkdirs(ps)
          logger.info("hdfs path name ||"+ps.getName+"hdfs create dir suceess"+mkflag)
        } else {
          logger.info("tar OutputStream entry.getName  " + entry.getName + "  || outputDir:  " + outputDir)
          val tarEntryName: String = entry.getName
          val flag: Boolean =CommonUtils.boolFilePrefixContains(tarEntryName,propertiesPath)
          if (flag == true) {
            val pss: Path = new Path(outputDir + "/" + entry.getName)
            val out: FSDataOutputStream = fs.create(pss)
            try {
              var length = 0
              val arrayBuffer: Array[Byte] = new Array[Byte](8192*20)
              val loop: Breaks = new Breaks
              //              import loop.{break,breakable}
              //              breakable { }
              while ( {
                try {
                  length = tarln.read(arrayBuffer);
                  length != -1
                } catch {
                  case e: IndexOutOfBoundsException => {
                    false
                  }
                }

              }) {
                val ale = arrayBuffer.length
                if (arrayBuffer != null && ale > 0 && length >= 0 && ale >= length) {
                  out.write(arrayBuffer, 0, length)
                } else {
                  loop.break()
                }
              }
            } catch {
              case e: Exception => e.printStackTrace()
            } finally {
              out.flush()
              out.close()
              IOUtils.closeStream(out)
            }
          }
        }
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {

      }
    }
    IOUtils.closeStream(tarln)
    //fs.close()

  }

  /**
    * 解压  zip Bz2 gzip 类压缩文件
    *
    * @param srcDir
    * @param outputDir
    * @param fs
    */
  def zipBz2gzipFileUnCompress(srcDir: String, outputDir: String, fs: HDFSFileSystem): Unit = {
    val srcTemp: String = srcDir.toLowerCase()
    val is: InputStream =FileUtils.zipBz2gzipFileStreamBySuffix(srcDir, fs)
    if (srcTemp.endsWith(".zip")) {
      while (is.asInstanceOf[ZipInputStream].getNextEntry != null) {
        try {
          val entry: ZipEntry = is.asInstanceOf[ZipInputStream].getNextEntry
          if (entry.isDirectory) {
            logger.info("zip stream entry.getName " + entry.getName + "outputDir" + outputDir)
            val ps: Path = new Path(outputDir + "/" + entry.getName)
            fs.mkdirs(ps)
          } else {
            val out: FSDataOutputStream = fs.create(new Path(outputDir + "/" + entry.getName))
            try {
              var length: Int = 0
              val buff: Array[Byte] = new Array[Byte](2048)
              while ( {
                length = is.read(buff);
                length != -1
              }) {
                val ale = buff.length
                if (buff != null && ale > 0 && length >= 0 && ale >= length) {
                  out.write(buff, 0, length)
                }
              }
            } catch {
              case e: Exception => e.printStackTrace()
            } finally {
              IOUtils.closeStream(out)
            }
          }
        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          IOUtils.closeStream(is)
          // fs.close()
        }
      }
    } else {
      val fileName: String =CommonUtils.getFileName(srcDir)
      val pass: Path = new Path(outputDir + "/" + fileName)
      val out: FSDataOutputStream = fs.create(pass)
      try {
        var length: Int = 0
        val arrayBuffer: Array[Byte] = new Array[Byte](8192)
        while ( {
          length = is.read(arrayBuffer);
          length != -1
        }) {
          val ale = arrayBuffer.length
          if (arrayBuffer != null && ale > 0 && length >= 0 && ale >= length) {
            out.write(arrayBuffer, 0, length)
          }
        }
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        IOUtils.closeStream(out)
      }
    }
    IOUtils.closeStream(is)
    // fs.close()
  }

  def boolTarFileSuffix(srcDir:String):Boolean={
    if(srcDir.endsWith(".tar.gz")||srcDir.endsWith(".tar.bz2")||srcDir.endsWith(".tgz")||srcDir.endsWith(".tar")){
      return  true
    }else{
      return false
    }
  }
  def  boolCompressFileSuffix(srcTemp:String):Boolean={
    if (srcTemp.endsWith(".bz2")||srcTemp.endsWith(".gz")||srcTemp.endsWith(".zip")) {
      return  true
    }else{
      return false
    }

  }




  def umcompresstar(srcDir: String, outputDir: String, fs: HDFSFileSystem,propertiesPath:String): Unit = {
    var tarln: TarInputStream = null
    var is: InputStream = null
    logger.info("umcompresstar method execing")
    try {
      val srcTemp: String = srcDir.toLowerCase()
      if(boolTarFileSuffix(srcTemp)){
        logger.info("srctemp tar.gz uncompressing")
        tarln =FileUtils.tarfileStreamBySuffix(srcTemp,fs)
        logger.info("tarball  umcompress successfully")
      }else if (boolCompressFileSuffix(srcTemp)){
        is =FileUtils.zipBz2gzipFileStreamBySuffix(srcTemp,fs)
      }

      if (boolTarFileSuffix(srcTemp)) {
        logger.info("begin uncompress log data tar gz")
        var entry: TarEntry = null
        while ( {
          try{
            entry = tarln.getNextEntry; entry != null
          }catch {
            case e:Exception => false
          }
        }) {
          try {
            if (entry.isDirectory) {
              logger.info("tar entry.getName  " + entry.getName + "   || outputDir:  " + outputDir)
              val ps: Path = new Path(outputDir + "/" + entry.getName)
              fs.mkdirs(ps)
            } else {
              logger.info("tar OutputStream entry.getName  " + entry.getName + "  || outputDir:  " + outputDir)
              val tarEntryName: String = entry.getName
              //if (in.contains("biz.log") || in.contains("info.log") || in.contains("ad_status") || in.contains("ad_behavior"))
              var flag:Boolean=false
              val loop:Breaks =new Breaks
              val proSeq=PropertiesUtils.converOuterPropertiesToSeq(propertiesPath)(",")
              loop.breakable {
                proSeq.foreach(fliePrefix=>{
                  val suffixContain=tarEntryName.contains(fliePrefix)
                  logger.info("bool fliePrefix || " + fliePrefix + " res  || " + suffixContain)
                  if (suffixContain==true){
                    logger.info(" suffixContain   && "+suffixContain)
                    flag=true
                    loop.break()
                  }
                })

              }
              logger.info("flag || "+ flag)
              if(flag==true) {
                val pss: Path = new Path(outputDir + "/" + entry.getName)
                val out: FSDataOutputStream = fs.create(pss)
                try {
                  var length = 0
                  val arrayBuffer: Array[Byte] = new Array[Byte](8192)
                  val loop: Breaks = new Breaks
                  //              import loop.{break,breakable}
                  //              breakable { }
                  while ( {
                    try {
                      length = tarln.read(arrayBuffer);
                      length != -1
                    }catch {
                      case e : IndexOutOfBoundsException => {
                        logger.info("read length"+length)
                        false
                      }
                    }

                  }) {
                    val ale = arrayBuffer.length
                    //println(" arrayBuffer "+ale+" length "+ length)
                    if (arrayBuffer != null && ale > 0 && length >= 0 && ale >= length) {
                      out.write(arrayBuffer, 0, length)
                    } else {
                      loop.break()
                    }
                  }

                } catch {
                  case e: Exception => e.printStackTrace()
                } finally {
                  IOUtils.closeStream(out)
                }
              }
            }
          } catch {
            case e: Exception => e.printStackTrace()
          }

        }
      } else if (srcTemp.endsWith(".zip")) {

        while (is.asInstanceOf[ZipInputStream].getNextEntry != null) {
          try {

            val entry: ZipEntry = is.asInstanceOf[ZipInputStream].getNextEntry
            if (entry.isDirectory) {
              logger.info("zip stream entry.getName " + entry.getName + "outputDir" + outputDir)
              val ps: Path = new Path(outputDir + "/" + entry.getName)
              fs.mkdirs(ps)
            } else {
              val out: FSDataOutputStream = fs.create(new Path(outputDir + "/" + entry.getName))
              try {
                var length: Int = 0
                val buff: Array[Byte] = new Array[Byte](2048)
                while ( {
                  length = is.read(buff); length != -1
                }) {
                  val ale = buff.length
                  if (buff != null && ale > 0 && length >= 0 && ale >= length) {
                    out.write(buff, 0, length)
                  }
                }

              } catch {
                case e: Exception => e.printStackTrace()
              } finally {

                IOUtils.closeStream(out)
              }

            }
          } catch {
            case e: Exception => e.printStackTrace()
          }


        }
      } else {
        val fileName: String =CommonUtils.getFileName(srcDir)
        val pass: Path = new Path(outputDir + "/" + fileName)
        val out: FSDataOutputStream = fs.create(pass)
        try {
          var length: Int = 0
          val arrayBuffer: Array[Byte] = new Array[Byte](8192)
          while ( {
            length = is.read(arrayBuffer); length != -1
          }) {
            val ale = arrayBuffer.length
            if (arrayBuffer != null && ale > 0 && length >= 0 && ale >= length) {
              out.write(arrayBuffer, 0, length)
            }
          }
        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          IOUtils.closeStream(out)
        }
      }

    } finally {
      IOUtils.closeStream(tarln)
      IOUtils.closeStream(is)
      //fs.close()
    }
  }


}
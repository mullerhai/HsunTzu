package com.HsunTzu.utils

import java.io.InputStream
import java.util.zip.{GZIPInputStream, ZipInputStream}

import com.typesafe.scalalogging.Logger
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream
import org.apache.hadoop.fs.{FSDataInputStream, Path, FileSystem => HDFSFileSystem}
import org.apache.tools.tar.TarInputStream
import org.slf4j.LoggerFactory

class FileUtils {

}

object  FileUtils{

  private [this] val logger=Logger(LoggerFactory.getLogger(classOf[FileUtils]))

  /** *
    * 根据 tar包 后缀 选择解压数据io 流对象
    *
    * @param srcDir
    * @param fs
    * @return
    */
  def tarfileStreamBySuffix(srcDir: String, fs: HDFSFileSystem): TarInputStream = {
    var tarln: TarInputStream = null
    val srcTemp: String = srcDir.toLowerCase()
    try {
      val fsData: FSDataInputStream = fs.open(new Path(srcDir))
      if (srcTemp.endsWith(".tar.gz")) {
        logger.info("srctemp tar.gz uncompressing" + fs.getUri + srcDir + "fs" + fs)
        tarln = new TarInputStream(new GZIPInputStream(fsData))
        logger.info("tarball  umcompress successfully")
      } else if (srcTemp.endsWith(".tar.bz2")) {
        tarln = new TarInputStream(new BZip2CompressorInputStream(fsData))
      } else if (srcTemp.endsWith(".tgz")) {
        tarln = new TarInputStream(new GZIPInputStream(fsData))
      } else if (srcTemp.endsWith(".tar")) {
        tarln = new TarInputStream(fsData)
      }
    } catch {
      case e: Exception => throw e
    }
    return tarln
  }

  /**
    * 根据 压缩文件后缀 选择解压数据流方式
    *
    * @param srcDir
    * @param fs
    * @return
    */
  def zipBz2gzipFileStreamBySuffix(srcDir: String, fs: HDFSFileSystem): InputStream = {
    var is: InputStream = null
    val srcTemp: String = srcDir.toLowerCase()
    try {
      if (srcTemp.endsWith(".bz2")) {
        is = new BZip2CompressorInputStream(fs.open(new Path(srcDir)))
      } else if (srcTemp.endsWith(".gz")) {
        is = new GZIPInputStream(fs.open(new Path(srcDir)))
      } else if (srcTemp.endsWith(".zip")) {
        is = new ZipInputStream(fs.open(new Path(srcDir)))
      }
    } catch {
      case e: Exception => throw e
    }
    return is
  }

}
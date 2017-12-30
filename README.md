# HsunTzu

# version 
## Beat 1.0

#  Very Fast  Hdfs  Origin File  To  Compress  Decompress Untar Tarball 

# LISENCE
##  MIT

#  工欲善其事必先利其器

![avatar](https://timgsa.baidu.com/timg?image&quality=80&size=b9999_10000&sec=1514649799522&di=447db98a2ec75e64828d4f09540924c3&imgtype=0&src=http%3A%2F%2Fimgtu.lishiquwen.com%2F20160924%2F9d3c1aa228ede64a7d615b17b64d73f0.jpg)

# Good tools are prerequisite to the successful execution of a job

# First 
##  You  need   install  jdk 8   scala 2.12.1 +  sbt 1.0.4 +  hadoop 2.8.1 +  ,
##  also   you can edit the version  on build.sbt and ./project/build.properties

# Get 

## git clone git@github.com:mullerhai/HsunTzu.git 
## cd  ./HsunTzu 

# Compile

##  sbt clean compile

# Package

## sbt update
## sbt assembly

# Run 

## hadoop jar    HsunTzuPro-beat-2.0.jar   inputPath   outPath   CompressType    PropertiesFilePath    inputCodec   OutputCodec 

## you will see the  logger info  on  console output 

## something. before run.  you need to know. 

## CompressType.  use. number instead of. compress method invoke 
 
   ###  case "1" => exec.originFileToCompressFile
   ###  case "2" => exec.tarFileToOriginFile
   ###  case "3" => exec.compressFileToOriginFile
   ###  case "4" => exec.oneCompressConvertOtherCompress



## compress codec  use num instead of. codec class  ,example. use. 1 instead of snappycodec 
  ###  case "0" => deflateCode
  ###  case "1" => snappyCode
  ###  case "2" => gzipCode
  ###  case "3" => lz4Code
  ###  case "4" => bZip2Code
  ###  case "5" => defaultCode
  ###  case _ =>  deflateCode
    
##  PropertiesFilePath

###  you need create one property file , example.   /usr/local/info.properties
###  you need  decleard  the  file prefix  in. Key.[files]. of your property file. for. select  compress or decompress or untar

###  if. you want to. decleard. the. hdfs address and port and operetion user ,you need.  to fill the file. in. property file
###  like this. intention. the key.  must. like this  [ hdfsAddr  hdfsPort FsKey. FsUserKey. hadoopUser HDFSPORTDOTSUFFIX. files] !!
#### hdfsAddr=hdfs://192.168.255.161:9000
#### hdfsPort=9000
#### FsKey=fs.defaultFS
#### FsUserKey=HADOOP_USER_NAME
#### hadoopUser=linkedme_hadoop
#### HDFSPORTDOTSUFFIX= :9000/
#### files=biz,ad_status,ad_behavior

### argument you need to. declard six ,maybe. the last argument is not can use. 
## rum example. 
###  hadoop jar   HsunTzuPro-beat-2.0.jar    /facishare-data/taru/20170820   /facishare-data/gao  1  /usr/local/info.properties 1  0

### this. is just for  originFile.to  CompressFile, and. compress file use. snappyCodec. 

###  hadoop jar HsunTzuPro-beat-2.0.jar /facishare-data/taruns/taruns/tarun/20170820/    /facishare-data/xin   4  /usr/local/info.properties  0  1

### this is just. for  deflateCodec compress files. convert to snappyCodec Compress files.  oneCompressConvertOtherCompress


###   hadoop jar HsunTzuPro-beat-2.0.jar /facishare-data/taruns/taruns/tarun/20170820/    /facishare-data/xin   3  /usr/local/info.properties  3  0

### this is just  lz4Codec Compress files. Decompress. to. origin files


###    hadoop jar HsunTzuPro-beat-2.0.jar /facishare-data/taruns/taruns/tarun/20170820/    /facishare-data/xin   2  /usr/local/info.properties  0  0

###  this is just tarball file. to origin file 




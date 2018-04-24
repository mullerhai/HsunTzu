HsunTzu
=======

[![Apache 2.0](https://img.shields.io/github/license/nebula-plugins/nebula-project-plugin.svg)](http://www.apache.org/licenses/LICENSE-2.0)
[![Build Status](https://travis-ci.org/jtablesaw/tablesaw.svg?branch=master)](https://travis-ci.org/jtablesaw/tablesaw)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/5029f48d00c24f1ea378b090210cf7da)](https://www.codacy.com/app/jtablesaw/tablesaw?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=jtablesaw/tablesaw&amp;utm_campaign=Badge_Grade)

### Overview


### version  Beat 2.0


## Very Fast  Hdfs  Origin File  To  Compress  Decompress Untar Tarball 

![avatar](https://github.com/mullerhai/sshjumphive/blob/master/hsuntzu.jpg)
##  LISENCE.  Apache 2.0


###  工欲善其事必先利其器  --荀子 HSUNTZU

#### 这个工具主要是 应用在HDFS上 做文件及日志的压缩归档 和逆操作 解压 等，支持 多目录同时 并行 压缩，支持HDFS 现有的六种压缩格式，经测试 在PB级数据上完全没有问题，该压缩工具使用不会占用  MapReduce Job队列，友好支持在  shell repl 中运行，也支持 集成到独立的项目中，使用前请确认 贵司 的HDFS集群环境，需要配置一下集群的地址 等等信息 ，在运行命令时，也需要指定必要的参数，比如  要压缩的文件路径 操作后的输出路径，压缩类型  配置文件路径 输入的压缩格式 输出的压缩格式，项目还在不断添加新的功能中，欢迎大家踊跃尝试，解决 HDFS上的文件归档痛点，释放 HDFS更大的空间。现在支持四种完美的类型，1.原始文件被压2.原始文件打包 ，3，tar包文件 解压为原始文件 4，批量目录文件的压缩或打包,不建议在mac 和Windows系统尝试，建议使用 centos 服务系统

#### 优势

#### 快：  支持并行 |不占MapReduce 任务队列  稳 ：不会中途宕机终止 | 性能占用 平稳    准 ： 确保数据的完整性  |不丢失 不重复  不损坏  多 ： 支持 HDFS 现存的六种压缩格式 和 直接在 HDFS 打包 不经 本地系统   可配置  可复用  可集成 

###  大数据治理 思想

####  普通人 比较关注 这个工具的 压缩率到底是多少 ，我可以简单回答一下 snappy 大概是3-5 倍   gzip default defalte 大概是 6 -12倍  bzip 大概是 4-7倍  lz4 大概是 4-5倍， 然鹅儿， 这并不全面 ，具体的压缩比率和 你的文件内容也有非常大的关系，单独谈压缩率都是耍流氓。 我们 不该只谈论某一种性能的优势，我们在处理大数据的时候，要考虑很多实际问题，比如  集群容量规划 ，压缩解压打包 过程 对集群性能的占用和损耗  未来归档数据 被计算是否支持split ，压缩时间消耗， 未来的跨集群调度数据  集群扩容 数据 Rebalance 等等，很多时候这是一个相互妥协的过程 ，需要权衡 需要谨慎定夺 考虑 到底该如何选择


#### 回答一些疑问
#####  是新的压缩算法嘛？ ---->  不是的，是现有的 HDFS 六种压缩算法的完全封装 和 Apache Commons Tar Api 的封装调用，并没有自研新压缩算法，自研新的算法 ，关键hadoop不支持 也是白扯

#####  hadoop 难道没有这些命令 嘛？  --->  hadoop 只定义开放了API，自己别没有去做封装实现，我做大数据治理多年，假如hadoop 有的话，我又何必造轮子呢，这个压缩 打包正是 大数据处理的一个痛点 难点， hadoop 自有的 HAR 类型 只打包 并不压缩

##### 单纯 调用人家API 没有技术含量呀？ -->  呵呵，你很牛逼 ，期待你自己整个开源的 让大家使用嘛

##### 你这东西也就是半产品 吧？   --->  你只要会用，哪怕是一坨幸运 也可以在你的项目中闪闪发光的 带来价值

##### 这个未来会加入 新功能嘛？   --->  会的，我会一直维护，

##### 这个工具可靠嘛？   --->  当然 了，已经在多位 大厂 经过PB级生产环境的考验，包括 DIDI  360 的数据平台

![avatar](https://timgsa.baidu.com/timg?image&quality=80&size=b9999_10000&sec=1514649799522&di=447db98a2ec75e64828d4f09540924c3&imgtype=0&src=http%3A%2F%2Fimgtu.lishiquwen.com%2F20160924%2F9d3c1aa228ede64a7d615b17b64d73f0.jpg)

###  Good tools are prerequisite to the successful execution of a job

#   How To Use It   ! ! ! 
##  Shell Command model & argument  like below content: [MAYBE YOU NEED  EDIT CONFIGFILE FIRST]
### HadoopExecPath    jar     HsunTzuPro-beat-2.0.jar    InputDir OutputDir     OperateType     ConfigFilePath     InputCodec    OutputCodec
### hadoop执行脚本  jar   HsunTzuPro-beat-2.0.jar  待压缩/打包/解压的输入文件目录   解压/解包/压缩的输出文件目录  操作类型  配置文件路径  输入的压缩格式  输出的压缩格式


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
 
   ###  case "1"  => exec.originFileToCompressFile
   ###  case "2" => exec.tarFileToOriginFile
   ###  case "3" => exec.compressFileToOriginFile
   ###  case "4" => exec.oneCompressConvertOtherCompress
 



## compress codec  use number instead of. codec class  ,example. use. 1 instead of snappycodec 
  ###  case "0" => deflateCode
  ###  case "1" => snappyCodec
  ###  case "2" => gzipCodec
  ###  case "3" => lz4Codec
  ###  case "4" => bZip2Codec
  ###  case "5" => defaultCodec
  ###  case _ =>  deflateCodec
    
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
## run example. 

###  hadoop jar   HsunTzuPro-beat-2.0.jar    /facishare-data/taru/20170820   /facishare-data/gao  1  /usr/local/info.properties 1  0

### this. is just for  originFile.to  CompressFile, and. compress file use. snappyCodec. 

###  hadoop jar HsunTzuPro-beat-2.0.jar /facishare-data/taruns/taruns/tarun/20170820/    /facishare-data/xin   4  /usr/local/info.properties  0  1

### this is just. for  deflateCodec compress files. convert to snappyCodec Compress files.  oneCompressConvertOtherCompress


###   hadoop jar HsunTzuPro-beat-2.0.jar /facishare-data/taruns/taruns/tarun/20170820/    /facishare-data/xin   3  /usr/local/info.properties  3  0

### this is just  lz4Codec Compress files. Decompress. to. origin files


###    hadoop jar HsunTzuPro-beat-2.0.jar /facishare-data/taruns/taruns/tarun/20170820/    /facishare-data/xin   2  /usr/local/info.properties  0  0

###  this is just tarball file. to origin file 




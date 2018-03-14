# HsunTzu

# version 
## Beat 1.0

#  Very Fast  Hdfs  Origin File  To  Compress  Decompress Untar Tarball 

# LISENCE
##  MIT

#  工欲善其事必先利其器  ---  荀子

###  这个工具主要是 应用在HDFS上 做文件及日志的压缩归档 和逆操作 解压 等，支持 多目录同时 并行 压缩，支持HDFS 现有的六种压缩格式，经测试 在PB级数据上完全没有问题，该压缩工具使用不会占用  MapReduce Job队列，友好支持在  shell repl 中运行，也支持 集成到独立的项目中，使用前请确认 贵司 的HDFS集群环境，需要配置一下集群的地址 等等信息 ，在运行命令时，也需要指定必要的参数，比如  要压缩的文件路径 操作后的输出路径，压缩类型  配置文件路径 输入的压缩格式 输出的压缩格式，项目还在不断添加新的功能中，欢迎大家踊跃尝试，解决 HDFS上的文件归档痛点，释放 HDFS更大的空间。现在支持四种完美的类型，1.原始文件被压2.原始文件打包 ，3，tar包文件 解压为原始文件  4，批量目录文件的压缩或打包



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

## hadoop jarHsunTzuPro-beat-2.0.jar  inputPath outPath CompressType  PropertiesFilePath  inputCodec  OutputCodec 

## you will see the  logger info  on  console output 


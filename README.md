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

## hadoop jarHsunTzuPro-beat-2.0.jar  inputPath outPath CompressType  PropertiesFilePath  inputCodec  OutputCodec 

## you will see the  logger info  on  console output 


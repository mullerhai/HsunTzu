# HsunTzu

#  Very Fast  Hdfs  Origin File  To  Compress  Decompress Untar Tarball 

#  工欲善其事必先利其器

# Good tools are prerequisite to the successful execution of a job

# First 
##  You  need   install  jdk 8   scala 2.12.1 +  sbt 1.0.4 +  hadoop 2.8.1 +  ,
##  also   you can edit the version  on build.sbt and ./project/build.properties

# Get 

## git clone git@github.com:mullerhai/HsunTzu.git 
## cd  ./HsunTzu 

# Compile

##  sbt clean compile

# fat Package

## sbt update
## sbt assembly

# Run 

## hadoop jarHsunTzuPro-beat-2.0.jar  inputPath outPath CompressType  PropertiesFilePath  inputCodec  OutputCodec 

## you will see the  logger info  on  console output 


name := "HsunTzu"

version := "0.9"

scalaVersion := "2.12.1"
libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-common" % "2.8.1",
  "org.apache.hadoop" % "hadoop-hdfs" % "2.8.1",
  "org.apache.hadoop" % "hadoop-client" % "2.8.1",
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "2.8.1",
  "org.apache.hadoop" % "hadoop-mapreduce-client-common" % "2.8.1",
  // https://mvnrepository.com/artifact/org.apache.ant/ant
  "org.apache.ant" % "ant" % "1.10.1"
  // https://mvnrepository.com/artifact/org.kamranzafar/jtar
  //"org.kamranzafar" % "jtar" % "2.3"
)

lazy val loggingSettings = Seq(
  libraryDependencies ++= Seq(
    "com.typesafe.scala-logging" %% "scala-logging"   % "3.7.2",
    "ch.qos.logback"             %  "logback-classic" % "1.2.3")
)

lazy val commonSettings = loggingSettings ++ Seq(
  // Plugin that prints better implicit resolution errors.
  addCompilerPlugin("io.tryp"  % "splain" % "0.2.7" cross CrossVersion.patch)
)

lazy val testSettings = Seq(
  libraryDependencies ++= Seq(
    "junit"         %  "junit" %   "4.12",
    "org.scalactic" %% "scalactic" % "3.0.4",
    "org.scalatest" %% "scalatest" % "3.0.4" % "test"),
  logBuffered in Test := false,
  fork in test := false,
  testForkedParallel in Test := false,
  parallelExecution in Test := false,
  testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oDF")
)
resolvers += Resolver.sonatypeRepo("snapshots")
libraryDependencies += "org.platanios" %% "tensorflow" % "0.1.0-SNAPSHOT"
mainClass := Some("com.HsunTzu.exec.execCompress")
unmanagedResourceDirectories in Compile += baseDirectory.value /"src/main/resources"
resourceDirectory in Compile := baseDirectory.value / "src/webapp"
resourceDirectory in Compile := baseDirectory.value / "src/main/resource"

assemblyOutputPath in assembly := baseDirectory.value/"HsunTzuPro-beat-2.0.jar"
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
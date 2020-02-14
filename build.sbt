name := "SparkExp"

version := "0.1"

scalaVersion := "2.11.12"

//useCoursier := false

//logLevel := Level.Debug

resolvers ++= Seq(
//  ("aliyun" at "http://maven.aliyun.com/nexus/content/groups/public").withAllowInsecureProtocol(allowInsecureProtocol = true)
//  ("aliyun" at "https://maven.aliyun.com/repository/central")
)

libraryDependencies ++= Seq(
  "commons-io" % "commons-io" % "2.6",
  "org.glassfish.jaxb" % "txw2" % "2.3.2",
  "org.slf4j" % "slf4j-api" % "1.7.25",
  "org.scalatest" %% "scalatest" % "3.0.8" withSources() withJavadoc(),
  "com.novocode" % "junit-interface" % "0.11",
  "org.apache.spark" %% "spark-core" % "2.4.4" withSources() withJavadoc(),
  "org.apache.spark" %% "spark-sql" % "2.4.4" withSources() withJavadoc(),
  "org.scala-lang" % "scala-library" % scalaVersion.value,
  "com.databricks" %% "spark-xml" % "0.8.0" withSources() withJavadoc(),
  //   https://mvnrepository.com/artifact/mysql/mysql-connector-java
  "mysql" % "mysql-connector-java" % "8.0.19"
)
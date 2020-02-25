name := "SparkExp"

version := "0.1"

scalaVersion := "2.12.10"

//useCoursier := false

logLevel := Level.Debug

resolvers ++= Seq(
//  ("aliyun" at "http://maven.aliyun.com/nexus/content/groups/public").withAllowInsecureProtocol(allowInsecureProtocol = true)
//  ("aliyun" at "https://maven.aliyun.com/repository/central")
)
libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-api" % "1.7.25",
  "org.scalatest" %% "scalatest" % "3.0.8" withSources(),
  "org.apache.spark" %% "spark-core" % "2.4.4" withSources(),
  "org.apache.spark" %% "spark-sql" % "2.4.4" withSources(),
  "org.scala-lang" % "scala-library" % scalaVersion.value,
  "com.databricks" %% "spark-xml" % "0.8.0" withSources(),
  // https://mvnrepository.com/artifact/mysql/mysql-connector-java
  "mysql" % "mysql-connector-java" % "5.1.48",
  "com.databricks" %% "spark-xml" % "0.8.0" withSources(),
  // https://mvnrepository.com/artifact/mysql/mysql-connector-java
  "mysql" % "mysql-connector-java" % "5.1.48",
  "org.mongodb.spark" %% "mongo-spark-connector" % "2.4.0",
  "org.apache.spark" %% "spark-core" % "2.4.0",
  "org.apache.spark" %% "spark-sql" % "2.4.0",
  //"org.scalatest" %% "scalatest" % "3.1.1"
)

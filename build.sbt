name := "SparkExp"

version := "0.1"

scalaVersion := "2.13.11"

libraryDependencies ++= Seq(
  "commons-io" % "commons-io" % "2.6",
  "org.glassfish.jaxb" % "txw2" % "2.3.2",
  "org.slf4j" % "slf4j-api" % "1.7.25" ,
  "org.scalatest" %% "scalatest" % "3.0.8" ,
  "com.novocode" % "junit-interface" % "0.11" ,
  "org.apache.spark" %% "spark-core" % "2.4.4" ,
  "org.apache.spark" %% "spark-sql" % "2.4.4" ,
  "org.scala-lang" % "scala-library" % scalaVersion.value,
  "com.databricks" %% "spark-xml" % "0.8.0",
//   https://mvnrepository.com/artifact/mysql/mysql-connector-java
  "mysql" % "mysql-connector-java" % "8.0.19"
)
name := "analyze_blockchain"

version := "1.0"

scalaVersion := "2.11.7"
val sparkVersion = "2.1.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion, //% "provided"
  "org.apache.spark" %% "spark-sql" % sparkVersion //% "provided"
  //"org.apache.spark" % "spark-hive_2.10" % "2.1.0",
  //"org.apache.spark" % "spark-yarn_2.10", % "2.1.0"
)
    
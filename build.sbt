name := "sbtkafka"

scalaVersion := "2.11.11"

organization := "com.qps.sbtkafka"

autoScalaLibrary := false

libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % "1.6.2",
    "org.apache.kafka" %% "kafka" % "0.10.0.0"
)

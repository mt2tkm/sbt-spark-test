name := "sbtsparktest"

version := "1.0"

scalaVersion := "2.11.7"

classpathTypes += "maven-plugin"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "1.6.0",
  "org.apache.spark" % "spark-sql_2.11"  % "1.6.0"
  //"org.apache.spark" % "spark-mllib_2.11" % "1.6.0"
)

dependencyOverrides ++= Set(
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.4"
)

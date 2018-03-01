name := "equoid-data-handler"

organization := "io.radanalytics"

version := "0.1.0-SNAPSNOT"

scalaVersion := "2.11.8"

crossScalaVersions := Seq("2.11.8")

val sparkVersion = "2.2.1"

licenses += ("Apache-2.0", url("http://opensource.org/licenses/Apache-2.0"))

def commonSettings = Seq(
  libraryDependencies ++= Seq(
    "org.infinispan" % "infinispan-core" % "9.1.4.Final",
    "org.infinispan" % "infinispan-client-hotrod" % "9.1.4.Final",
    "org.infinispan" %% "infinispan-spark" % "0.6",    
    "io.radanalytics" %% "spark-streaming-amqp" % "0.3.1",
    "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
    "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
    "org.apache.spark" %% "spark-streaming" % sparkVersion % Provided,
    "org.scalatest" %% "scalatest" % "3.0.4" % Test,
    "org.apache.commons" % "commons-math3" % "3.6.1" % Test),
    scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")
)

seq(commonSettings:_*)

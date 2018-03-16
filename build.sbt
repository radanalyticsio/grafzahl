name := "equoid-data-handler"

organization := "io.radanalytics"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.11.8"

crossScalaVersions := Seq("2.11.8")

val sparkVersion = "2.2.1"

licenses += ("Apache-2.0", url("http://opensource.org/licenses/Apache-2.0"))

def commonSettings = Seq(
  libraryDependencies ++= Seq(
    "org.infinispan" % "infinispan-core" % "9.1.4.Final",
    "org.infinispan" % "infinispan-client-hotrod" % "9.1.4.Final",
    "org.infinispan" %% "infinispan-spark" % "0.6",    
    ("io.radanalytics" %% "spark-streaming-amqp" % "0.3.1").exclude("com.fasterxml.jackson.core", "jackson-databind"),
    "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
    "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
    "org.apache.spark" %% "spark-streaming" % sparkVersion % Provided,
    "org.scalatest" %% "scalatest" % "3.0.4" % Test,
    "org.apache.commons" % "commons-math3" % "3.6.1" % Test),
    scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")
)

seq(commonSettings:_*)

test in assembly := {}

// not sure what strategy to use for these
// see https://github.com/sbt/sbt-assembly
assemblyMergeStrategy in assembly := {
  case "META-INF/DEPENDENCIES.txt" => MergeStrategy.discard
  case "META-INF/io.netty.versions.properties" => MergeStrategy.discard
  case "features.xml" => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

assemblyShadeRules in assembly := Seq(
  // ShadeRule.zap("scala.**").inAll,
  // ShadeRule.zap("org.slf4j.**").inAll
)


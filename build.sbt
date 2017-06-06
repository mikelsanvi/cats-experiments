
name := "cats-experiments"

version := "1.0.0-SNAPSHOT"

scalaVersion := "2.11.11"

lazy val sparkVersion = "2.1.0"


libraryDependencies in ThisProject ++= Seq(
  "org.typelevel" %% "cats" % "0.9.0",
  "com.chuusai" %% "shapeless" % "2.3.2",
  "org.apache.spark" %% "spark-core" % sparkVersion withSources () withJavadoc (),
  "org.apache.spark" %% "spark-sql" % sparkVersion withSources () withJavadoc ()
)


name := "cats-experiments"

version := "1.0.0-SNAPSHOT"

scalaVersion := "2.11.11"

lazy val sparkVersion = "2.1.0"

lazy val scalaTestVersion = "2.2.5"

libraryDependencies in ThisProject ++= Seq(
  "org.typelevel" %% "cats" % "0.9.0",
  "com.chuusai" %% "shapeless" % "2.3.2",
  "org.scalameta" %% "scalameta" % "1.8.0" % Provided,
  "org.apache.spark" %% "spark-core" % sparkVersion withSources () withJavadoc (),
  "org.apache.spark" %% "spark-sql" % sparkVersion withSources () withJavadoc (),
  "org.scalatest" %% "scalatest" % scalaTestVersion % Test
)

addCompilerPlugin("org.scalameta" % "paradise" % "3.0.0-M10" cross CrossVersion.full)
scalacOptions += "-Xplugin-require:macroparadise"
scalacOptions in (Compile, console) ~= (_ filterNot (_ contains "paradise")) // macroparadise plugin doesn't work in repl yet.

name := "mexico-economic-profile-spark"

version := "0.1"

scalaVersion := "2.12.18" // Ensure compatibility with Spark 3.x

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.4.1",
  "org.apache.spark" %% "spark-sql" % "3.4.1"
)

ThisBuild / organization := "com.alejandro"

// Optional: Enable packaging JAR
//enablePlugins(JavaAppPackaging)

// Optional: Assembly for fat JAR
// addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "1.1.0")
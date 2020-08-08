lazy val root = project
  .in(file("."))
  .settings(
    name := "CovidSocialImpact",

    version := "0.1",

    scalaVersion := "2.12.12",

    libraryDependencies ++= Seq(
      "com.novocode" % "junit-interface" % "0.11" % Test,
      "org.apache.spark" %% "spark-core" % "3.0.0",
      "org.apache.spark" %% "spark-sql" % "3.0.0",
      "org.apache.spark" %% "spark-mllib" % "3.0.0",
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
      "com.github.fommil.netlib" % "all" % "1.1.2" pomOnly()
    ),

    testOptions in Test += Tests.Argument(TestFrameworks.JUnit, "-a", "-v", "-s")
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
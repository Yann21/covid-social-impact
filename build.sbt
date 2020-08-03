lazy val root = project
  .in(file("."))
  .settings(
    name := "CovidSocialImpact",

    version := "0.1",

    scalaVersion := "2.12.12",

    libraryDependencies ++= Seq(
      "com.novocode" % "junit-interface" % "0.11" % Test,
      ("org.apache.spark" %% "spark-core" % "3.0.0"),
      ("org.apache.spark" %% "spark-sql" % "3.0.0"),
      ("org.apache.spark" %% "spark-mllib" % "3.0.0")
    ),

    testOptions in Test += Tests.Argument(TestFrameworks.JUnit, "-a", "-v", "-s")
)
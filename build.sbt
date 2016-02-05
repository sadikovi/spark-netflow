name := "spark-netflow"

organization := "com.github.sadikovi"

scalaVersion := "2.10.5"

crossScalaVersions := Seq("2.10.5", "2.11.7")

spName := "sadikovi/spark-netflow"

val defaultSparkVersion = "1.4.1"

sparkVersion := sys.props.getOrElse("spark.testVersion", defaultSparkVersion)

val defaultHadoopVersion = "2.6.0"

val hadoopVersion = settingKey[String]("The version of Hadoop to test against.")

hadoopVersion := sys.props.getOrElse("hadoop.testVersion", defaultHadoopVersion)

spAppendScalaVersion := true

spIncludeMaven := false

spIgnoreProvided := true

sparkComponents := Seq("sql")

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.4" % "test"
)

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-client" % hadoopVersion.value % "test" exclude("javax.servlet", "servlet-api") force(),
  "org.apache.spark" %% "spark-core" % sparkVersion.value % "test" exclude("org.apache.hadoop", "hadoop-client"),
  "org.apache.spark" %% "spark-sql" % sparkVersion.value % "test" exclude("org.apache.hadoop", "hadoop-client")
)

// Display full-length stacktraces from ScalaTest:
testOptions in Test += Tests.Argument("-oF")

parallelExecution in Test := false

// Skip tests during assembly
test in assembly := {}

ScoverageSbtPlugin.ScoverageKeys.coverageHighlighting := {
  if (scalaBinaryVersion.value == "2.10") false
  else true
}

EclipseKeys.eclipseOutput := Some("target/eclipse")

// tasks dependencies
lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")
compileScalastyle := org.scalastyle.sbt.ScalastylePlugin.scalastyle.in(Compile).toTask("").value
(compile in Compile) <<= (compile in Compile).dependsOn(compileScalastyle)

// Create a default Scala style task to run with tests
lazy val testScalastyle = taskKey[Unit]("testScalastyle")
testScalastyle := org.scalastyle.sbt.ScalastylePlugin.scalastyle.in(Test).toTask("").value
(test in Test) <<= (test in Test).dependsOn(testScalastyle)

/********************
 * Release settings *
 ********************/

publishMavenStyle := true

releaseCrossBuild := true

licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))

releasePublishArtifactsAction := PgpKeys.publishSigned.value

pomExtra := (
  <url>https://github.com/sadikovi/spark-netflow</url>
  <scm>
    <url>git@github.com:sadikovi/spark-netflow.git</url>
    <connection>scm:git:git@github.com:sadikovi/spark-netflow.git</connection>
  </scm>
  <developers>
    <developer>
      <id>sadikovi</id>
      <name>Ivan Sadikov</name>
      <url>https://github.com/sadikovi</url>
    </developer>
  </developers>
)

bintrayReleaseOnPublish in ThisBuild := false

import ReleaseTransformations._

// Add publishing to spark packages as another step.
releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  publishArtifacts,
  setNextVersion,
  commitNextVersion,
  pushChanges,
  releaseStepTask(spPublish)
)

credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")

lazy val root = (project in file(".")).
  settings(
    name := "realtime-analyzer",
    version := "1.0",
    scalaVersion := "2.10.4",
    mainClass in Compile := Some("de.haw.bachelorthesis.dkirchner.RealtimeAnalyzer"),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "1.3.0" % "provided",
      "org.apache.spark" % "spark-mllib_2.10" % "1.3.0",
      "org.apache.spark" % "spark-streaming_2.10" % "1.3.0",
      "org.apache.spark" % "spark-streaming-twitter_2.10" % "1.3.0"
    )
  )

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

// META-INF discarding
mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
{
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
}
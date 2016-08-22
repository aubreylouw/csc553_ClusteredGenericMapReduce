lazy val root = (project in file(".")).
  settings (
    name := "MapReduceClient",
    version := "1.0",
    scalaVersion := "2.11.8",
    scalacOptions in ThisBuild ++= Seq("-unchecked", "-deprecation"),
    resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
    libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.4.4",
    libraryDependencies += "com.typesafe.akka" %% "akka-remote" % "2.4.4",
    libraryDependencies += "com.typesafe.akka" %% "akka-cluster" % "2.4.4",
    libraryDependencies += "com.typesafe.akka" %% "akka-cluster-tools" % "2.4.4",
    mainClass in (Compile, run) := Some("mapreduceclient.MapReduceClientApp")
  )
logLevel := Level.Warn

resolvers ++= Seq(
  "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
  "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/",
  "JBoss" at "https://repository.jboss.org",
  "Akka Repository" at "http://repo.akka.io/releases/",
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"
)

// plugins
addSbtPlugin("org.spark-packages" % "sbt-spark-package" % "0.2.5")
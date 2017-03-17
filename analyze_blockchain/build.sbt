name := "analyze_blockchain"

version := "1.0"

scalaVersion := "2.11.7"

resolvers ++= Seq(
  "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
  "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/",
  "spark-packages" at "https://dl.bintray.com/spark-packages/maven/",
  "JBoss" at "https://repository.jboss.org",
  "Akka Repository" at "http://repo.akka.io/releases/"
)
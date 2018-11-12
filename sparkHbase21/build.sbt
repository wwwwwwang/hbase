
name := "sparkHbase21"

version := "0.1"

scalaVersion := "2.10.6"

val sparkVersion = "1.6.2"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % sparkVersion % "provided"
libraryDependencies += "org.apache.hbase" % "hbase-client" % "2.1.0"
libraryDependencies += "org.apache.hbase" % "hbase-server" % "2.1.0"
libraryDependencies += "org.apache.hbase" % "hbase-common" % "2.1.0"
libraryDependencies += "org.apache.hbase" % "hbase-mapreduce" % "2.1.0"

libraryDependencies += "com.typesafe" % "config" % "1.3.0"
libraryDependencies += "commons-logging" % "commons-logging" % "1.1.1"
libraryDependencies += "log4j" % "log4j" % "1.2.17"

resolvers ++= Seq(
  "Maven Repository" at "http://repo.maven.apache.org/maven2",
  "Apache Repository" at "https://repository.apache.org/content/repositories/releases",
  "JBoss Repository" at "https://repository.jboss.org/nexus/content/repositories/releases/",
  "MQTT Repository" at "https://repo.eclipse.org/content/repositories/paho-releases/",
  "Cloudera Repository" at "http://repository.cloudera.com/artifactory/cloudera-repos/",
  Resolver.mavenLocal
)

assemblyMergeStrategy in assembly := {
  case PathList(ps@_*) if ps.last endsWith ".html" => MergeStrategy.first
  case PathList(ps@_*) if ps.last endsWith ".class" => MergeStrategy.first
  case PathList(ps@_*) if ps.last endsWith ".xsd" => MergeStrategy.first
  case PathList(ps@_*) if ps.last endsWith ".dtd" => MergeStrategy.first
  case "application.conf" => MergeStrategy.concat
  case "rootdoc.txt" => MergeStrategy.first
  case x => {
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
  }
}
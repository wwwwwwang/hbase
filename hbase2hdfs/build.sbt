name := "hbase2hdfs"

version := "1.0"

scalaVersion := "2.10.6"

val sparkVersion = "1.6.2"
val hadoopVersion = "2.0.0-mr1-cdh4.7.0"

libraryDependencies += "org.apache.spark" % "spark-core_2.10"  % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" % "spark-mllib_2.10" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" % "spark-sql_2.10"   % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" % "spark-hive_2.10"  % sparkVersion % "provided"
libraryDependencies += "org.apache.hbase" % "hbase"             % "0.94.15-cdh4.7.0"  excludeAll(
  ExclusionRule("org.apache.hadoop", "hadoop-common"),
  ExclusionRule("org.jruby"),
  ExclusionRule("io.netty"),
  ExclusionRule("org.apache.zookeeper"),
  ExclusionRule("org.mortbay.jetty"),
  ExclusionRule("com.sun.jersey"),
  ExclusionRule("tomcat"),
  ExclusionRule("commons-configuration"),
  ExclusionRule("org.slf4j"))
/*libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.2.0-cdh5.11.1"
libraryDependencies += "org.apache.hbase" % "hbase-server" % "1.2.0-cdh5.11.1"
libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.2.0-cdh5.11.1"*/
libraryDependencies += "com.typesafe"      % "config"            % "1.3.0"
libraryDependencies += "net.minidev"       % "json-smart"        % "1.3.1"
libraryDependencies += "commons-logging"   % "commons-logging"  % "1.1.1"
libraryDependencies += "log4j" % "log4j" % "1.2.17"

resolvers ++= Seq(
  // HTTPS is unavailable for Maven Central
  "Maven Repository"     at "http://repo.maven.apache.org/maven2",
  "Apache Repository"    at "https://repository.apache.org/content/repositories/releases",
  "JBoss Repository"     at "https://repository.jboss.org/nexus/content/repositories/releases/",
  "MQTT Repository" at "https://repo.eclipse.org/content/repositories/paho-releases/",
  "Cloudera Repository"  at "http://repository.cloudera.com/artifactory/cloudera-repos/",
  // For Sonatype publishing
  // "sonatype-snapshots"   at "https://oss.sonatype.org/content/repositories/snapshots",
  // "sonatype-staging"     at "https://oss.sonatype.org/service/local/staging/deploy/maven2/",
  // also check the local Maven repository ~/.m2
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
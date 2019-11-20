name := "Eurowings-2"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies  +=  "org.apache.spark"  %%  "spark-core"  %  "2.4.3"
libraryDependencies  +=  "org.apache.spark"  %%  "spark-sql"  %  "2.4.3"
// https://mvnrepository.com/artifact/mysql/mysql-connector-java
libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.48"

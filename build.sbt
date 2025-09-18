ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.7.2"

lazy val core = (project in file("modules/core"))
  .settings(
    name := "kentavro-core",
    libraryDependencies ++= List(
      deps.avro,
      deps.scalaTest
    )
  )

lazy val idl = (project in file("modules/idl"))
  .settings(
    name := "kentavro-idl",
    libraryDependencies ++= List(
      deps.avroIdl,
      deps.scalaTest
    )
  )
  .dependsOn(core)

lazy val example = (project in file("example"))
  .enablePlugins(BuildInfoPlugin)
  .settings(
    name := "kentavro-example",
    buildInfoOptions += BuildInfoOption.ConstantValue,
    buildInfoKeys ++= Seq[BuildInfoKey](
      "rootDir" -> baseDirectory.value.toString
    ),
    buildInfoPackage := "kentavro.example"
  )
  .dependsOn(idl)

lazy val root = (project in file("."))
  .settings(
    name := "kentavro"
  )
  .aggregate(
    core,
    idl,
    example
  )

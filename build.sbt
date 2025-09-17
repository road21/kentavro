ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.7.2"

lazy val core = (project in file("modules/core"))
  .settings(
    name := "kentavro-core",
    libraryDependencies ++= List(
      deps.avro
    )
  )

lazy val idl = (project in file("modules/idl"))
  .settings(
    name := "kentavro-idl",
    libraryDependencies ++= List(
      deps.avroIdl
    )
  )
  .dependsOn(core)

lazy val example = (project in file("example"))
  .settings(
    name := "kentavro-example",
    Compile / sourceGenerators += Def.task {
      val outputDir     = (Compile / sourceManaged).value / "kentavro" / "example"
      val generatedFile = outputDir / "BuildInfo.scala"
      val rootDir       = baseDirectory.value.toString

      // Ensure the output directory exists
      IO.createDirectory(outputDir)

      // Write the content to the file
      val content =
        s"""package kentavro.example
          |
          |object BuildInfo:
          |  val rootDir: "$rootDir" = "$rootDir"
          |""".stripMargin
      IO.write(generatedFile, content)

      Seq(generatedFile)
    }.taskValue
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

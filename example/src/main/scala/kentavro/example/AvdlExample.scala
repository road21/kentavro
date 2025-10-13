package kentavro.example

import kentavro.{Avdl, Named}
import kentavro.example.BuildInfo

@main def run(): Unit =
  val userSchema =
    Avdl
      .withImports(BuildInfo.rootDir + "/models")
      .fromFileIn("user.avdl")

  val bytes = userSchema.serialize(
    Named.make("John", 1, "john@example.com", 30, Named.make("123 Main St", "Anytown", "12345"))
  )

  println(userSchema.deserialize(bytes).map(_.value.id))

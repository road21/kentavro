package kentavro.example

import kentavro.{Avdl, Named}
import kentavro.example.BuildInfo
import scala.annotation.experimental

@experimental
@main def run(): Unit =
  val s =
    Avdl
      .withImports(BuildInfo.rootDir + "/models")
      .fromFileIn("user.avdl")

  val bytes = s.serialize(s.User("Lol", 42, "", 42, s.Address("", "", ""), s.Status.ACTIVE))
  println(s.deserialize(bytes).map(_.value.age))

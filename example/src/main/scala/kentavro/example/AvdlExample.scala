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

  import s.`com.example.kentavro.Address` as address

  val bytes = s.serialize(s.User(
    "Vlad",
    42,
    "@",
    42,
    address("city", "768"),
    s.Status.SUSPENDED
  ))

  // val bytes = s.serialize(s.User("Lol", 42, "", 42, s.Address("", "", ""), s.Status.ACTIVE))
  val user = s.User
  s.deserialize(bytes).map { x =>
    println(x.value.status)
    println(s.Status.SUSPENDED)
    x.value.status match
      case s.Status.ACTIVE    => "act"
      case s.Status.SUSPENDED => "act"
      case s.Status.PENDING   => "pend"
      case s.Status.INACTIVE  => "sd"
  }

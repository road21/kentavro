package kentavro.example

import kentavro.Avdl
import kentavro.example.BuildInfo
import scala.annotation.experimental

@experimental
@main def run(): Unit =
  val s =
    Avdl
      .withImports(BuildInfo.rootDir + "/models")
      .fromFileIn("user.avdl")

  val ns    = s.`com.example.kentavro`
  val bytes = s.serialize(
    s.User(
      "John",
      1,
      null,
      42,
      ns.Address("city", "768"),
      s.Status.SUSPENDED
    )
  )

  println(
    s.deserialize(bytes).map { x =>
      x.value.status.value match
        case s.Status.ACTIVE.value    => "act"
        case s.Status.SUSPENDED.value => "sus"
        case s.Status.PENDING.value   => "pend"
        case s.Status.INACTIVE.value  => "sd"
    }
  )

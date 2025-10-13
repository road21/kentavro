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
    ns.User(
      "John",
      1,
      "@email",
      42,
      ns.Address("city", "768"),
      ns.Status.SUSPENDED
    )
  )

  println(
    s.deserialize(bytes).map { x =>
      x.value.status.value match
        case ns.Status.ACTIVE.value    => "act"
        case ns.Status.SUSPENDED.value => "sus"
        case ns.Status.PENDING.value   => "pend"
        case ns.Status.INACTIVE.value  => "sd"
    }
  )

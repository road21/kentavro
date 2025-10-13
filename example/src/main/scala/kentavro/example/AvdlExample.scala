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

  println(s.`com.example.kentavro`.values)
  // val bytes = s.serialize(
  //   s.`com.example.kentavro.User`(
  //     "John",
  //     1,
  //     "@email",
  //     42,
  //     s.`com.example.kentavro.Address`("city", "768"),
  //     s.`com.example.kentavro.Status`.SUSPENDED
  //   )
  // )

  println(
    ()
    // s.deserialize(bytes).map { x =>
    //   val st = s.`com.example.kentavro.Status`
    //   x.value.status.value match
    //     case st.ACTIVE.value    => "act"
    //     case st.SUSPENDED.value => "sus"
    //     case st.PENDING.value   => "pend"
    //     case st.INACTIVE.value  => "sd"
    // }
  )

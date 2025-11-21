package kentavro.example

import kentavro.*
import kentavro.Avdl
import kentavro.Named
import kentavro.example.BuildInfo.rootDir

import scala.annotation.experimental

@experimental
@main def run(): Unit =
  val schema = Avdl.fromFile(rootDir + "/models/user.avdl")
  val ns = schema.`example.fscala`
  val bytes = schema.serialize(ns.User("Bob", 42, ns.Active((msg = "hi"))))
  schema.deserialize(bytes).map { x => 
    val v = x.value
    
    val y: 
      "example.fscala.Active" ~ (msg : String) | 
      "example.fscala.Inactive" ~ (lastSeen : Long, kek: String) = v.status
    
    type m = (msg: String)
    y match
      case ns.Inactive.name ~ ns.Inactive.value(lastSeen = x, kek = y) => ???
      case _ => ??? //ns.Inactive.name ~ (lastSeen) => lastSeen
  }

  //schema.serialize(schema)

  //val user = s.User("Bob", 42)

  ()

  // val bytes = s.serialize(
  //   s.User
  //   s.User(
  //     "John",
  //     1,
  //     null,
  //     42,
  //     ns.Address("city", "768"),
  //     s.Status.SUSPENDED
  //   )
  // )

  // println(
  //   s.deserialize(bytes).map { x =>
  //     x.value.status.value match
  //       case s.Status.ACTIVE.value    => "act"
  //       case s.Status.SUSPENDED.value => "sus"
  //       case s.Status.PENDING.value   => "pend"
  //       case s.Status.INACTIVE.value  => "sd"
  //   }
  // )

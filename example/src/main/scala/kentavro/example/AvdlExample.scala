package kentavro.example

import kentavro.{Avdl, Named}
import kentavro.example.BuildInfo
import kentavro.AvroReader
import kentavro.AvroWriter

case class Address(
  street: String,
  city: String,
  zipCode: String
)

object Address:
  transparent inline given AvroWriter.Struct.Aux[Address, ?] = 
    val foo = AvroWriter.Struct.derived[Address]
      .rename("com.example.avro.Address")
      .renameField("zipCode", "zip")
    foo

case class User(
  name: String,
  id: Int, 
  email: String,
  age: Int,
  address: Address
)

import kentavro.~
object User:
  given AvroWriter.Struct[User] = 
    val s = AvroWriter.Struct.derived[User]
    summon[s.FieldValues =:= ("com.example.avro.Address" ~ (street: String, city: String, zip: String))]
    ???
    //summon[s.FieldValues =:= (String, Int, String, Int, ))]

@main def run(): Unit =
  val userSchema =
    Avdl
      .withImports(BuildInfo.rootDir + "/models")
      .fromFileIn("user.avdl")

  val bytes = userSchema.serialize(
    Named.make("John", 1, "john@example.com", 30, Named.make("123 Main St", "Anytown", "12345"))
  )

  println(userSchema.deserialize(bytes).map(_.value.id))

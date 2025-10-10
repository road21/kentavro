package kentavro

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.avro.Schema
import kentavro.Utils.stripMarginCT
import org.apache.avro.Schema.Type
import scala.annotation.experimental

@experimental
class AvdlTest extends AnyFlatSpec with Matchers:
  it should "be able to parse record schemas" in:
    val recordSch: KSchema["com.example.avro.User" ~ (id: Int, name: String, email: String, age: Int)] =
      Avdl.fromString(
        """|namespace com.example.avro;
           |schema User;
           |record User {
           |    int id;
           |    string name;
           |    string email;
           |    int age;
           |}
           |""".stripMarginCT
      )

    recordSch.avroType match
      case r: AvroType.RecordSchema[?, ?] =>
        r.fields should have size 4
        r.fields.map(f => (f.name, f.schema.schema)) should be(
          List(
            "id"    -> Schema.create(Type.INT),
            "name"  -> Schema.create(Type.STRING),
            "email" -> Schema.create(Type.STRING),
            "age"   -> Schema.create(Type.INT)
          )
        )
      case _ =>
        fail("Expected record schema")

  it should "be able to parse nested record schemas" in:
    val recordSch: KSchema["com.example.avro.User" ~ (
        id: Int,
        name: String,
        email: String,
        age: Int,
        address: "com.example.avro.Address" ~ (country: String, city: String, street: String)
    )] =
      Avdl.fromString(
        """|namespace com.example.avro;
           |schema User;
           |
           |record Address {
           |    string country;
           |    string city;
           |    string street;
           |}
           |
           |record User {
           |    int id;
           |    string name;
           |    string email;
           |    int age;
           |    Address address;
           |}
           |""".stripMarginCT
      )

    recordSch.avroType match
      case r: AvroType.RecordSchema[?, ?] =>
        r.fields should have size 5
        r.fields.collectFirst {
          case AvroType.Field("address", AvroType.RecordSchema(fields, _), _) => fields
        }.fold(
          fail("Expected record schema")
        ) {
          _.map(f => (f.name, f.schema.schema)) should be(
            List(
              "country" -> Schema.create(Type.STRING),
              "city"    -> Schema.create(Type.STRING),
              "street"  -> Schema.create(Type.STRING)
            )
          )
        }
      case _ =>
        fail("Expected record schema")

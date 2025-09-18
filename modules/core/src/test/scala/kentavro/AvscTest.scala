package kentavro

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import kentavro.Utils.stripMarginCT

class AvscTest extends AnyFlatSpec with Matchers:
  it should "be able to parse a primitive schemas" in:
    val stringSch: KSchema[String] = Avsc.fromString("""{"type": "string"}""")
    stringSch.schema should be(Schema.create(Type.STRING))

    val intSch: KSchema[Int] = Avsc.fromString("""{"type": "int"}""")
    intSch.schema should be(Schema.create(Type.INT))

  it should "be able to parse record schemas" in:
    val recordSch: KSchema[(id: Int, name: String, email: String, age: Int)] =
      Avsc.fromString(
        """|{
           |  "namespace": "example.avro",
           |  "type": "record",
           |  "name": "User",
           |  "fields": [
           |    {
           |      "name": "id",
           |      "type": "int"
           |    },
           |    {
           |      "name": "name",
           |      "type": "string"
           |    },
           |    {
           |      "name": "email",
           |      "type": "string"
           |    },
           |    {
           |      "name": "age",
           |      "type": "int"
           |    }
           |  ]
           |}
           |""".stripMarginCT
      )

    recordSch match
      case r: KSchema.Record[?] =>
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
    val recordSch: KSchema[(
        id: Int,
        name: String,
        email: String,
        age: Int,
        address: (country: String, city: String, street: String)
    )] =
      Avsc.fromString(
        """|{
           |  "namespace": "example.avro",
           |  "type": "record",
           |  "name": "User",
           |  "fields": [
           |    {
           |      "name": "id",
           |      "type": "int"
           |    },
           |    {
           |      "name": "name",
           |      "type": "string"
           |    },
           |    {
           |      "name": "email",
           |      "type": "string"
           |    },
           |    {
           |      "name": "age",
           |      "type": "int"
           |    },
           |    {
           |      "name": "address",
           |      "type": {
           |        "type": "record",
           |        "name": "Address",
           |        "fields": [
           |          {
           |            "name": "country",
           |            "type": "string"
           |          },
           |          {
           |            "name": "city",
           |            "type": "string"
           |          },
           |          {
           |            "name": "street",
           |            "type": "string"
           |          }
           |        ]
           |      }
           |    }
           |  ]
           |}
           |""".stripMarginCT
      )

    recordSch match
      case r: KSchema.Record[?] =>
        r.fields should have size 5
        r.fields.collectFirst {
          case KSchema.Field("address", KSchema.Record(fields, _, _)) => fields
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

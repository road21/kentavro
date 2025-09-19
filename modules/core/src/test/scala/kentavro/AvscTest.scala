package kentavro

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import kentavro.Utils.stripMarginCT
import NamedTuple.withNames

class AvscTest extends AnyFlatSpec with Matchers:
  it should "be able to parse a primitive schemas" in:
    val nullSch: KSchema[Null] = Avsc.fromString("""{"type": "null"}""")
    nullSch.schema should be(Schema.create(Type.NULL))

    val boolSch: KSchema[Boolean] = Avsc.fromString("""{"type": "boolean"}""")
    boolSch.schema should be(Schema.create(Type.BOOLEAN))

    val intSch: KSchema[Int] = Avsc.fromString("""{"type": "int"}""")
    intSch.schema should be(Schema.create(Type.INT))

    val longSch: KSchema[Long] = Avsc.fromString("""{"type": "long"}""")
    longSch.schema should be(Schema.create(Type.LONG))

    val floatSch: KSchema[Float] = Avsc.fromString("""{"type": "float"}""")
    floatSch.schema should be(Schema.create(Type.FLOAT))

    val doubleSch: KSchema[Double] = Avsc.fromString("""{"type": "double"}""")
    doubleSch.schema should be(Schema.create(Type.DOUBLE))

    val bytesSch: KSchema[Array[Byte]] = Avsc.fromString("""{"type": "bytes"}""")
    bytesSch.schema should be(Schema.create(Type.BYTES))

    val stringSch: KSchema[String] = Avsc.fromString("""{"type": "string"}""")
    stringSch.schema should be(Schema.create(Type.STRING))

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
          case KSchema.Field("address", KSchema.Record(fields, _), _) => fields
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

  it should "respect round-trip serialization for primitive fields" in:
    type Primitives = (
        fieldNull: Null,
        fieldBoolean: Boolean,
        fieldInt: Int,
        fieldLong: Long,
        fieldFloat: Float,
        fieldDouble: Double,
        fieldBytes: Array[Byte],
        fieldString: String
    )
    val primitives: KSchema[Primitives] =
      Avsc.fromString(
        """|{
           |  "namespace": "example.avro",
           |  "type": "record",
           |  "name": "Primitives",
           |  "fields": [
           |    {
           |      "name": "fieldNull",
           |      "type": "null"
           |    },
           |    {
           |      "name": "fieldBoolean",
           |      "type": "boolean"
           |    },
           |    {
           |      "name": "fieldInt",
           |      "type": "int"
           |    },
           |    {
           |      "name": "fieldLong",
           |      "type": "long"
           |    },
           |    {
           |      "name": "fieldFloat",
           |      "type": "float"
           |    },
           |    {
           |      "name": "fieldDouble",
           |      "type": "double"
           |    },
           |    {
           |      "name": "fieldBytes",
           |      "type": "bytes"
           |    },
           |    {
           |      "name": "fieldString",
           |      "type": "string"
           |    }
           |  ]
           |}
           |""".stripMarginCT
      )

    val test: Primitives = (null, true, 1, 2L, 3.0f, 4.0, Array(1, 2, 3), "example")
    primitives.deserialize(primitives.serialize(test)) match
      case Right(
            fieldNull = fieldNull,
            fieldBoolean = fieldBoolean,
            fieldInt = fieldInt,
            fieldLong = fieldLong,
            fieldFloat = fieldFloat,
            fieldDouble = fieldDouble,
            fieldBytes = fieldBytes,
            fieldString = fieldString
          ) =>
        (fieldNull, fieldBoolean, fieldInt, fieldLong, fieldFloat, fieldDouble, fieldString) should be(
          (
            test.fieldNull,
            test.fieldBoolean,
            test.fieldInt,
            test.fieldLong,
            test.fieldFloat,
            test.fieldDouble,
            test.fieldString
          )
        )
        fieldBytes.sameElements(test.fieldBytes) should be(true)
      case _ =>
        fail("Expected successfull deserialization")

  it should "respect round-trip for array schemas" in:
    val stringArr: KSchema[Vector[String]] =
      Avsc.fromString(
        """|{
           |  "items": "string",
           |  "type": "array"
           |}
           |""".stripMarginCT
      )

    val arr = Vector("foo", "bar", "buzz")
    stringArr.deserialize(stringArr.serialize(arr)) should be(Right(arr))

    val usrsSch: KSchema[Vector[(id: Int, name: String)]] =
      Avsc.fromString(
        """|{
           |  "type": "array",
           |  "items": {
           |    "type": "record",
           |    "name": "LineItem",
           |      "fields": [
           |        {
           |          "name": "id",
           |          "type": "int"
           |        },
           |        {
           |          "name": "name",
           |          "type": "string"
           |        }
           |    ]
           |  }
           |}
           |""".stripMarginCT
      )

    val usrs = Vector[(id: Int, name: String)](1 -> "Bob", 2 -> "John", 3 -> "Cash")
    usrsSch.deserialize(usrsSch.serialize(usrs)) should be(Right(usrs))

  it should "respect round-trip for enums" in:
    val tlSchema: KSchema["RED" | "YELLOW" | "GREEN"] =
      Avsc.fromString(
        """|{
           |  "type": "enum",
           |  "name": "TrafficLight",
           |  "namespace": "com.example.enums",
           |  "symbols": ["RED", "YELLOW", "GREEN"],
           |  "doc": "Represents the states of a traffic light."
           |}
           |""".stripMarginCT
      )

    val red: "RED"       = valueOf
    val yellow: "YELLOW" = valueOf
    val green: "GREEN"   = valueOf

    tlSchema.deserialize(tlSchema.serialize(red)) should be(Right(red))
    tlSchema.deserialize(tlSchema.serialize(yellow)) should be(Right(yellow))
    tlSchema.deserialize(tlSchema.serialize(green)) should be(Right(green))

  it should "respect round-trip for map schemas" in:
    val intMap: KSchema[Map[String, Int]] =
      Avsc.fromString(
        """|{
           |  "type": "map",
           |  "values": "int"
           |}
           |""".stripMarginCT
      )

    val map = Map("foo" -> 1, "bar" -> 2, "buzz" -> 3)
    intMap.deserialize(intMap.serialize(map)) should be(Right(map))

    val usrsSch: KSchema[Map[String, (id: Int, name: String)]] =
      Avsc.fromString(
        """|{
           |  "type": "map",
           |  "values": {
           |    "type": "record",
           |    "name": "LineItem",
           |      "fields": [
           |        {
           |          "name": "id",
           |          "type": "int"
           |        },
           |        {
           |          "name": "name",
           |          "type": "string"
           |        }
           |    ]
           |  }
           |}
           |""".stripMarginCT
      )

    val usrs = Map[String, (id: Int, name: String)](
      "bob"  -> (1, "Bob"),
      "john" -> (2, "John"),
      "cash" -> (3, "Cash")
    )
    usrsSch.deserialize(usrsSch.serialize(usrs)) should be(Right(usrs))

// package kentavro

// import Utils.stripMarginCT
// import scala.annotation.experimental

// @experimental
// @main def run =
//   Avsc.fromString(
//     """|{
//            |  "namespace": "example.avro",
//            |  "type": "record",
//            |  "name": "User",
//            |  "fields": [
//            |    {
//            |      "name": "id",
//            |      "type": "int"
//            |    },
//            |    {
//            |      "name": "name",
//            |      "type": "string"
//            |    },
//            |    {
//            |      "name": "email",
//            |      "type": "string"
//            |    },
//            |    {
//            |      "name": "age",
//            |      "type": "int"
//            |    }
//            |  ]
//            |}
//            |""".stripMarginCT
//   )

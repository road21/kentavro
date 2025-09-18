import sbt.*

object deps {
  val avro = "org.apache.avro" % "avro" % "1.12.0"
  val avroIdl = "org.apache.avro" % "avro-idl" % "1.12.0"
  val scalaTest = "org.scalatest" %% "scalatest" % "3.2.19" % Test
}

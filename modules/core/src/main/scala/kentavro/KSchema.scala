package kentavro

import NamedTuple.AnyNamedTuple
import org.apache.avro.Schema

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}

import scala.util.Try

/**
  * Representation of {@link apache.avro.Schema Schema} that keeps information about types on type-level.
  * Type `T` corresponds to type of data that can be serialized/deserialized by this schema.
  * 
  * Mapping of schema types and Scala types in representation:
  *   - primitives -> primitive scala types (e.g. string avro will be represented as `KSchema[String]`)
  *   - records -> named tuples (names of fields are keys)
  *   - unions  -> union types
  */
trait KSchema[T]:
  type Type = T

  def schema: Schema

  def serialize(data: T): Array[Byte] =
    KSchema.serializeUnsafe(serializeToObject(data), schema)

  def deserialize(bytes: Array[Byte]): Either[String, T] =
    KSchema.deserializeUnsafe(bytes, schema).flatMap(deserializeFromObject)

  protected def serializeToObject(data: T): Object
  protected def deserializeFromObject(obj: Object): Either[String, T]

object KSchema:
  trait Primitive[T] extends KSchema[T]

  object Primitive:
    case class IntSchema(override val schema: Schema) extends Primitive[Int]:
      override protected def serializeToObject(data: Int): Object = data: Integer

      override protected def deserializeFromObject(
          obj: Object
      ): Either[String, Int] =
        if (obj.isInstanceOf[Int]) Right(obj.asInstanceOf[Int])
        else Left(s"Expected int, got ${obj.getClass()}")

    case class StringSchema(override val schema: Schema)
      extends Primitive[String]:
      override protected def serializeToObject(data: String): Object = data
      override protected def deserializeFromObject(
          obj: Object
      ): Either[String, String] =
        if (obj.isInstanceOf[org.apache.avro.util.Utf8])
          Right(obj.asInstanceOf[org.apache.avro.util.Utf8].toString())
        else Left(s"Expected string, got ${obj.getClass()}")

  case class Field[Name <: String & Singleton, T](
      name: Name,
      schema: KSchema[T]
  )

  case class Record[T <: AnyNamedTuple](
      fields: List[Field[?, ?]],
      default: Option[T],
      schema: Schema
  ) extends KSchema[T]:
    override protected def serializeToObject(data: T): Object =
      val record = new GenericData.Record(schema)
      val values = NamedTuple.toList(data.asInstanceOf)
      values.lazyZip(fields).foreach { case (value, field: Field[?, t]) =>
        record.put(
          field.name,
          field.schema.serializeToObject(value.asInstanceOf[t])
        )
      }
      record

    override protected def deserializeFromObject(
        obj: Object
    ): Either[String, T] =
      if (obj.isInstanceOf[GenericRecord])
        val g = obj.asInstanceOf[GenericRecord]
        val (errs, succ) = fields.partitionMap(f =>
          f.schema.deserializeFromObject(g.get(f.name))
        )
        Either.cond(
          errs.isEmpty,
          Tuple.fromArray(succ.toArray).asInstanceOf[T],
          errs.mkString(",")
        )
      else Left(s"Expected record, got: ${obj.getClass()}")

  private def serializeUnsafe(obj: Object, schema: Schema): Array[Byte] = {
    val writer  = new GenericDatumWriter[Object](schema)
    val out     = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get().binaryEncoder(out, null)
    writer.write(obj, encoder)
    encoder.flush()
    out.toByteArray()
  }

  private def deserializeUnsafe(
      bytes: Array[Byte],
      schema: Schema
  ): Either[String, Object] = Try {
    val reader  = new GenericDatumReader[Object](schema)
    val in      = new ByteArrayInputStream(bytes)
    val decoder = DecoderFactory.get().binaryDecoder(in, null)
    reader.read(null, decoder)
  }.toEither.left.map(_.getMessage())

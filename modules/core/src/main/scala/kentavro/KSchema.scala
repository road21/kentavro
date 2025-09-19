package kentavro

import NamedTuple.AnyNamedTuple
import org.apache.avro.Schema

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}

import scala.util.Try
import java.nio.ByteBuffer
import scala.reflect.ClassTag
import scala.jdk.CollectionConverters.*
import scala.collection.immutable.ListSet
import org.apache.avro.generic.GenericData.EnumSymbol
import kentavro.data.Fixed
import org.apache.avro.generic.GenericFixed
import org.apache.avro.specific.{SpecificData, SpecificDatumReader, SpecificDatumWriter, SpecificFixed}
import org.apache.avro.io.{DatumReader, DatumWriter}

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

  protected def serializeToObject(data: T): AnyRef
  protected def deserializeFromObject(obj: AnyRef): Either[String, T]

object KSchema:
  trait Primitive[T] extends KSchema[T]

  object Primitive:
    case class NullSchema(override val schema: Schema)
      extends Primitive[Null]:
      override protected def serializeToObject(data: Null): AnyRef = data
      override protected def deserializeFromObject(
          obj: AnyRef
      ): Either[String, Null] =
        Either.cond(obj == null, null, s"Expected null, got $obj")

    case class BooleanSchema(override val schema: Schema)
      extends Primitive[Boolean]:
      override protected def serializeToObject(data: Boolean): AnyRef                    = data: java.lang.Boolean
      override protected def deserializeFromObject(obj: AnyRef): Either[String, Boolean] =
        obj match
          case o: java.lang.Boolean => Right(o)
          case _                    => Left(s"Expected boolean, got ${obj.getClass()}")

    case class IntSchema(override val schema: Schema) extends Primitive[Int]:
      override protected def serializeToObject(data: Int): AnyRef = data: Integer

      override protected def deserializeFromObject(
          obj: AnyRef
      ): Either[String, Int] =
        obj match
          case i: Integer => Right(i)
          case _          => Left(s"Expected int, got ${obj.getClass()}")

    case class LongSchema(override val schema: Schema)
      extends Primitive[Long]:
      override protected def serializeToObject(data: Long): AnyRef                    = data: java.lang.Long
      override protected def deserializeFromObject(obj: AnyRef): Either[String, Long] =
        obj match
          case l: java.lang.Long => Right(obj.asInstanceOf[java.lang.Long])
          case _                 => Left(s"Expected long, got ${obj.getClass()}")

    case class FloatSchema(override val schema: Schema)
      extends Primitive[Float]:
      override protected def serializeToObject(data: Float): AnyRef                    = data: java.lang.Float
      override protected def deserializeFromObject(obj: AnyRef): Either[String, Float] =
        obj match
          case f: java.lang.Float => Right(f)
          case _                  => Left(s"Expected float, got ${obj.getClass()}")

    case class DoubleSchema(override val schema: Schema)
      extends Primitive[Double]:
      override protected def serializeToObject(data: Double): AnyRef = data: java.lang.Double
      override protected def deserializeFromObject(
          obj: AnyRef
      ): Either[String, Double] =
        obj match
          case d: java.lang.Double => Right(d)
          case _                   => Left(s"Expected double, got ${obj.getClass()}")

    case class ArrayByteSchema(override val schema: Schema)
      extends Primitive[Array[Byte]]:
      override protected def serializeToObject(data: Array[Byte]): AnyRef =
        ByteBuffer.wrap(data)
      override protected def deserializeFromObject(
          obj: AnyRef
      ): Either[String, Array[Byte]] =
        obj match
          case b: ByteBuffer => Right(b.array())
          case _             => Left(s"Expected Array[Byte] (ByteBuffer), got ${obj.getClass()}")

    case class StringSchema(override val schema: Schema)
      extends Primitive[String]:
      override protected def serializeToObject(data: String): AnyRef =
        org.apache.avro.util.Utf8(data)
      override protected def deserializeFromObject(
          obj: AnyRef
      ): Either[String, String] =
        obj match
          case s: org.apache.avro.util.Utf8 => Right(s.toString)
          case _                            => Left(s"Expected string, got ${obj.getClass()}")

  case class Field[Name <: String & Singleton, T](
      name: Name,
      schema: KSchema[T],
      default: Option[T] = None
  )

  case class Record[T <: AnyNamedTuple](
      fields: List[Field[?, ?]],
      schema: Schema
  ) extends KSchema[T]:
    override protected def serializeToObject(data: T): AnyRef =
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
      obj match
        case g: GenericRecord =>
          val (errs, succ) = fields.partitionMap(f =>
            f.schema.deserializeFromObject(g.get(f.name))
          )
          Either.cond(
            errs.isEmpty,
            Tuple.fromArray(succ.toArray).asInstanceOf[T],
            errs.mkString(",")
          )
        case _ => Left(s"Expected record, got: ${obj.getClass()}")

  case class ArraySchema[T: ClassTag](
      override val schema: Schema,
      element: KSchema[T]
  ) extends KSchema[Vector[T]]:
    override protected def serializeToObject(data: Vector[T]): AnyRef =
      data.map(element.serializeToObject).asJavaCollection

    override protected def deserializeFromObject(obj: AnyRef): Either[String, Vector[T]] =
      obj match
        case c: java.util.Collection[?] =>
          val arr           = c.asScala.toVector
          val (errs, succs) = arr.partitionMap(element.deserializeFromObject)
          if (errs.nonEmpty) Left(errs.mkString(", "))
          else Right(succs)
        case _ => Left(s"Expected array (java.util.Collection), got: ${obj.getClass()}")

  case class MapSchema[T](
      override val schema: Schema,
      value: KSchema[T]
  ) extends KSchema[Map[String, T]]:
    override protected def serializeToObject(data: Map[String, T]): AnyRef =
      data.mapValues(value.serializeToObject).toMap.asJava

    override protected def deserializeFromObject(obj: AnyRef): Either[String, Map[String, T]] =
      obj match
        case jm: java.util.Map[?, ?] =>
          val m             = jm.asScala
          val (errs, succs) = m.partitionMap {
            case (key: org.apache.avro.util.Utf8, v) =>
              value.deserializeFromObject(v).map(key.toString -> _)
            case (key, _) => Left(s"key $key is not a string")
          }
          if (errs.nonEmpty) Left(errs.mkString(", "))
          else Right(succs.toMap)
        case _ => Left(s"Expected map (java.util.Map), got: ${obj.getClass()}")

  case class EnumSchema[T <: String](
      override val schema: Schema,
      symbols: ListSet[String]
  ) extends KSchema[T]:
    override protected def serializeToObject(data: T): AnyRef =
      EnumSymbol(schema, data)

    override protected def deserializeFromObject(obj: AnyRef): Either[String, T] =
      obj match
        case e: EnumSymbol =>
          val str = e.toString
          Right(str.asInstanceOf[T])
        case _ => Left(s"Expected symbol (string), got ${obj.getClass()}")

  case class FixedSchema[T <: Int & Singleton](
      override val schema: Schema,
      size: Int
  )(using ValueOf[T]) extends KSchema[Fixed[T]]:
    override protected def serializeToObject(data: Fixed[T]): AnyRef =
      new SpecificFixed:
        self =>
        override val bytes: Array[Byte]                  = data.bytes
        override def getSchema(): org.apache.avro.Schema = schema

        val writer: DatumWriter[AnyRef]                             = new SpecificDatumWriter[AnyRef](schema)
        override def writeExternal(out: java.io.ObjectOutput): Unit =
          writer.write(self, SpecificData.getEncoder(out))

        val reader: DatumReader[AnyRef]                 = new SpecificDatumReader[AnyRef](schema)
        def readExternal(in: java.io.ObjectInput): Unit =
          reader.read(self, SpecificData.getDecoder(in))

    override protected def deserializeFromObject(
        obj: AnyRef
    ): Either[String, Fixed[T]] =
      obj match
        case b: GenericFixed =>
          val arr = b.bytes()
          Fixed.from[T](arr).toRight(s"Expected array of bytes with size $size, got: size ${arr.length}")
        case _ => Left(s"Expected Array[Byte] (ByteBuffer), got ${obj.getClass()}")

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
  ): Either[String, AnyRef] = Try {
    val reader  = new GenericDatumReader[AnyRef](schema)
    val in      = new ByteArrayInputStream(bytes)
    val decoder = DecoderFactory.get().binaryDecoder(in, null)
    reader.read(null, decoder)
  }.toEither.left.map(_.getMessage())

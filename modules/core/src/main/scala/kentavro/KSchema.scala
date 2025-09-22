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
import scala.compiletime.summonInline
import scala.annotation.nowarn

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

  type JType <: AnyRef
  def isJType(value: AnyRef): Option[JType]
  def JTypeClassName: String

  def isType(value: Any): Option[T]
  def TypeClassName: String

  def schema: Schema

  def serialize(data: T): Array[Byte] =
    KSchema.serializeUnsafe(serializeToJType(data), schema)

  def deserialize(bytes: Array[Byte]): Either[String, T] =
    KSchema.deserializeUnsafe(bytes, schema).flatMap {
      x =>
        isJType(x) match
          case Some(j) => deserializeFromJType(j)
          case _       => Left(s"Expected $JTypeClassName, got ${x.getClass()}")
    }

  def serializeToJType(data: T): JType
  def deserializeFromJType(obj: JType): Either[String, T]

object KSchema:
  type Aux[Type, _JType] = KSchema[Type] { type JType = _JType }

  trait WithJClassTag[T, _JType <: AnyRef](
      using
      jClassTag: ClassTag[_JType],
      classTag: ClassTag[T]
  ) extends KSchema[T]:
    override type JType = _JType

    override val JTypeClassName: String = jClassTag.runtimeClass.toString()
    override val TypeClassName: String  = classTag.runtimeClass.toString()

    override def isJType(value: AnyRef): Option[JType] =
      jClassTag.unapply(value)

    override def isType(value: Any): Option[T] =
      classTag.unapply(value)

  trait Primitive[T] extends KSchema[T]

  trait PrimitiveWithJClassTag[T, _JType <: AnyRef] extends WithJClassTag[T, _JType], Primitive[T]

  object Primitive:
    case class NullSchema(override val schema: Schema)
      extends Primitive[Null]:
      override type JType = Null

      override def isJType(value: AnyRef): Option[Null] =
        Option.when(value == null)(null)

      override def isType(value: Any): Option[Null] =
        Option.when(value == null)(null)
      override val JTypeClassName: String = "Null"
      override val TypeClassName: String  = "Null"

      override def serializeToJType(data: Null): Null                    = data
      override def deserializeFromJType(obj: Null): Either[String, Null] = Right(null)

    case class BooleanSchema(override val schema: Schema)
      extends PrimitiveWithJClassTag[Boolean, java.lang.Boolean]:
      override def serializeToJType(data: Boolean): JType                    = data
      override def deserializeFromJType(obj: JType): Either[String, Boolean] = Right(obj)

    case class IntSchema(override val schema: Schema) extends PrimitiveWithJClassTag[Int, java.lang.Integer]:
      override def serializeToJType(data: Int): java.lang.Integer          = data
      override def deserializeFromJType(obj: Integer): Either[String, Int] = Right(obj)

    case class LongSchema(override val schema: Schema) extends PrimitiveWithJClassTag[Long, java.lang.Long]:
      override def serializeToJType(data: Long): JType                    = data
      override def deserializeFromJType(obj: JType): Either[String, Long] = Right(obj)

    case class FloatSchema(override val schema: Schema) extends PrimitiveWithJClassTag[Float, java.lang.Float]:
      override def serializeToJType(data: Float): JType                    = data
      override def deserializeFromJType(obj: JType): Either[String, Float] = Right(obj)

    case class DoubleSchema(override val schema: Schema) extends PrimitiveWithJClassTag[Double, java.lang.Double]:
      override def serializeToJType(data: Double): JType                    = data
      override def deserializeFromJType(obj: JType): Either[String, Double] = Right(obj)

    case class ArrayByteSchema(override val schema: Schema) extends PrimitiveWithJClassTag[Array[Byte], ByteBuffer]:
      override def serializeToJType(data: Array[Byte]): JType                    = ByteBuffer.wrap(data)
      override def deserializeFromJType(obj: JType): Either[String, Array[Byte]] = Right(obj.array())

    case class StringSchema(override val schema: Schema)
      extends PrimitiveWithJClassTag[String, org.apache.avro.util.Utf8]:
      override def serializeToJType(data: String): JType                    = org.apache.avro.util.Utf8(data)
      override def deserializeFromJType(obj: JType): Either[String, String] = Right(obj.toString)

  case class Field[Name <: String & Singleton, T](
      name: Name,
      schema: KSchema[T],
      default: Option[T] = None
  )

  case class Record[T <: AnyNamedTuple](
      fields: List[Field[?, ?]],
      schema: Schema
  )(using ClassTag[T]) extends WithJClassTag[T, GenericRecord]:
    override def serializeToJType(data: T): JType =
      val record = new GenericData.Record(schema)
      val values = NamedTuple.toList(data.asInstanceOf)
      values.lazyZip(fields).foreach { case (value, field: Field[?, t]) =>
        record.put(
          field.name,
          field.schema.serializeToJType(value.asInstanceOf[t])
        )
      }
      record

    override def deserializeFromJType(
        obj: JType
    ): Either[String, T] =
      val (errs, succ) = fields.partitionMap { f =>
        val v = obj.get(f.name)
        f.schema.isJType(v) match
          case Some(j) => f.schema.deserializeFromJType(j)
          case None    => Left(s"Expected ${f.schema.JTypeClassName}, got: $v")
      }
      Either.cond(
        errs.isEmpty,
        Tuple.fromArray(succ.toArray).asInstanceOf[T],
        errs.mkString(",")
      )

  case class ArraySchema[T: ClassTag](
      override val schema: Schema,
      element: KSchema[T]
  ) extends WithJClassTag[Vector[T], java.util.Collection[?]]:
    override def serializeToJType(data: Vector[T]): java.util.Collection[?] =
      data.map(element.serializeToJType).asJavaCollection

    override def deserializeFromJType(obj: java.util.Collection[?]): Either[String, Vector[T]] =
      val arr           = obj.asScala.toVector
      val (errs, succs) = arr.partitionMap { x =>
        element.isJType(x) match
          case Some(j) => element.deserializeFromJType(j)
          case None    => Left(s"Expected ${element.JTypeClassName}, got: $x")
      }
      Either.cond(errs.isEmpty, succs, errs.mkString(", "))

  case class MapSchema[T](
      override val schema: Schema,
      value: KSchema[T]
  ) extends WithJClassTag[Map[String, T], java.util.Map[org.apache.avro.util.Utf8, ?]]:
    override def serializeToJType(data: Map[String, T]): java.util.Map[org.apache.avro.util.Utf8, ?] =
      data.map((k, v) =>
        org.apache.avro.util.Utf8(k) -> value.serializeToJType(v)
      ).toMap.asJava

    override def deserializeFromJType(
        obj: java.util.Map[org.apache.avro.util.Utf8, ?]
    ): Either[String, Map[String, T]] =
      val m             = obj.asScala
      val (errs, succs) = m.partitionMap {
        case (key, v) =>
          value.isJType(v) match
            case Some(j) => value.deserializeFromJType(j).map(key.toString -> _)
            case None    => Left(s"Expected ${value.JTypeClassName}, got: $key")
      }
      Either.cond(errs.isEmpty, succs.toMap, errs.mkString(", "))

  case class EnumSchema[T <: String](
      override val schema: Schema,
      symbols: ListSet[String]
  )(using ClassTag[T]) extends WithJClassTag[T, EnumSymbol]:
    override def serializeToJType(data: T): EnumSymbol =
      EnumSymbol(schema, data)

    override def deserializeFromJType(obj: EnumSymbol): Either[String, T] =
      val str = obj.toString
      Right(str.asInstanceOf[T])

  object EnumSchema:
    inline def make[T <: String](schema: Schema, symbols: ListSet[String]): EnumSchema[T] =
      EnumSchema[T](schema, symbols)(using summonInline[ClassTag[T]])

  case class FixedSchema[T <: Int & Singleton](
      override val schema: Schema,
      size: Int
  )(using ValueOf[T]) extends WithJClassTag[Fixed[T], GenericFixed]:
    override def serializeToJType(data: Fixed[T]): GenericFixed =
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

    override def deserializeFromJType(
        obj: GenericFixed
    ): Either[String, Fixed[T]] =
      val arr = obj.bytes()
      Fixed.from[T](arr).toRight(s"Expected array of bytes with size $size, got: size ${arr.length}")

  case class UnionSchema[T](
      override val schema: Schema,
      variants: List[KSchema[?]]
  ) extends KSchema[T]:
    override type JType = AnyRef

    override val JTypeClassName: String = variants.map(_.JTypeClassName).mkString("|")
    override def TypeClassName: String  = variants.map(_.TypeClassName).mkString("|")

    override def isJType(value: AnyRef): Option[JType] =
      variants.view.map(_.isJType(value)).collectFirst { case Some(v) => v }

    override def isType(value: Any): Option[T] =
      variants.view.map(_.isType(value)).collectFirst { case Some(v) => v.asInstanceOf[T] }

    @nowarn
    override def serializeToJType(data: T): JType =
      variants.view.map(s => s.isType(data).map(s -> _)).collectFirst {
        case Some((s: KSchema[T], v: T)) =>
          s.serializeToJType(v)
      }.getOrElse(throw new Exception("shouldn't be thrown"))

    @nowarn
    override def deserializeFromJType(obj: AnyRef): Either[String, T] =
      variants.view.map(s => s.isJType(obj).map(s -> _)).collectFirst {
        case Some((s: KSchema.Aux[?, T], v: T)) => s.deserializeFromJType(v).asInstanceOf[Either[String, T]]
      }.toRight(s"Unable to deserialize $obj, deserializer not found").flatten

  private def serializeUnsafe(obj: AnyRef, schema: Schema): Array[Byte] = {
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

package kentavro

import NamedTuple.AnyNamedTuple
import org.apache.avro.Schema

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, GenericFixed, GenericRecord}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}

import scala.util.Try
import java.nio.ByteBuffer
import scala.reflect.ClassTag
import scala.jdk.CollectionConverters.*
import scala.collection.immutable.ListSet
import org.apache.avro.generic.GenericData.EnumSymbol
import kentavro.BytesN
import org.apache.avro.specific.{SpecificData, SpecificDatumReader, SpecificDatumWriter, SpecificFixed}
import org.apache.avro.io.{DatumReader, DatumWriter}
import kentavro.KSchema.Tag.TaggedJValue
import scala.collection.immutable.SeqMap
import org.apache.avro.generic.GenericContainer

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
  type JType <: AnyRef
  def isJType(value: AnyRef): Option[JType]
  def JTypeClassName: String

  def schema: Schema

  def serialize(data: T): Array[Byte] =
    KSchema.serializeUnsafe(serializeToJType(data), schema)

  def deserialize(bytes: Array[Byte]): Either[String, T] =
    KSchema.deserializeUnsafe(bytes, schema).flatMap {
      x =>
        isJType(x) match
          case Some(j) => deserializeFromJType(j)
          case _       => Left(
              s"Expected $JTypeClassName, got ${x.toString()}: ${x.getClass()}"
            )
    }

  def serializeToJType(data: T): JType
  def deserializeFromJType(obj: JType): Either[String, T]

object KSchema:
  sealed trait Tag:
    type JType <: AnyRef
    type SType

  object Tag:
    case object NullTag extends Tag:
      type JType = Null
      type SType = Null
    case object BooleanTag extends Tag:
      type JType = java.lang.Boolean
      type SType = Boolean
    case object IntTag extends Tag:
      type JType = java.lang.Integer
      type SType = Int
    case object LongTag extends Tag:
      type JType = java.lang.Long
      type SType = Long
    case object FloatTag extends Tag:
      type JType = java.lang.Float
      type SType = Float
    case object DoubleTag extends Tag:
      type JType = java.lang.Double
      type SType = Double
    case object StringTag extends Tag:
      type JType = org.apache.avro.util.Utf8
      type SType = String
    case object ArrayBytesTag extends Tag:
      type JType = ByteBuffer
      type SType = Array[Byte]
    case object ArrayTag extends Tag:
      type JType = java.util.Collection[?]
      type SType = Vector[?]
    case object MapTag extends Tag:
      type JType = java.util.Map[?, ?]
      type SType = Map[?, ?]
    case class NamedTag(name: String) extends Tag:
      type JType = AnyRef
      type SType = ? ~ ?

    case class TaggedJValue(t: Tag)(val v: t.JType)
    case class TaggedSValue(t: Tag)(val v: t.SType)

    def fromJava(x: Any): Option[TaggedJValue] =
      // TODO: think about order
      x match
        case null                                           => Some(TaggedJValue(NullTag)(null))
        case v: java.lang.Boolean                           => Some(TaggedJValue(BooleanTag)(v))
        case v: java.lang.Integer                           => Some(TaggedJValue(IntTag)(v))
        case v: java.lang.Long                              => Some(TaggedJValue(LongTag)(v))
        case v: java.lang.Float                             => Some(TaggedJValue(FloatTag)(v))
        case v: java.lang.Double                            => Some(TaggedJValue(DoubleTag)(v))
        case v: ByteBuffer                                  => Some(TaggedJValue(ArrayBytesTag)(v))
        case v: org.apache.avro.util.Utf8                   => Some(TaggedJValue(StringTag)(v))
        case v: java.util.Collection[?]                     => Some(TaggedJValue(ArrayTag)(v))
        case v: java.util.Map[?, ?]                         => Some(TaggedJValue(MapTag)(v))
        case v: (EnumSymbol | GenericRecord | GenericFixed) =>
          Some(TaggedJValue(NamedTag(v.getSchema().getFullName()))(v))
        case _ => None

    def fromScala(x: Any): Option[TaggedSValue] =
      x match
        case null           => Some(TaggedSValue(NullTag)(null))
        case v: Boolean     => Some(TaggedSValue(BooleanTag)(v))
        case v: Int         => Some(TaggedSValue(IntTag)(v))
        case v: Long        => Some(TaggedSValue(LongTag)(v))
        case v: Float       => Some(TaggedSValue(FloatTag)(v))
        case v: Double      => Some(TaggedSValue(DoubleTag)(v))
        case v: Array[Byte] => Some(TaggedSValue(ArrayBytesTag)(v))
        case v: String      => Some(TaggedSValue(StringTag)(v))
        case v: Vector[?]   => Some(TaggedSValue(ArrayTag)(v))
        case v: Map[?, ?]   => Some(TaggedSValue(MapTag)(v))
        case v: (? ~ ?)     => Some(TaggedSValue(NamedTag(v.name))(v))

  trait NamedSchema[Name <: Singleton & String, T](using ValueOf[Name]) extends KSchema[Name ~ T]:
    final val name: Name = valueOf

  trait Primitive[_JType <: AnyRef, SType](val tag: Tag)(using tag.JType =:= _JType, tag.SType =:= SType)
    extends KSchema[SType]:
    override type JType = _JType

    override val JTypeClassName: String = tag.toString

    override def isJType(value: AnyRef): Option[JType] =
      Tag.fromJava(value).collect {
        case t @ TaggedJValue(`tag`) => t.v.asInstanceOf[tag.JType]
      }

  object Primitive:
    case class NullSchema(override val schema: Schema)
      extends Primitive(Tag.NullTag):
      override def serializeToJType(data: Null): Null                    = data
      override def deserializeFromJType(obj: Null): Either[String, Null] = Right(null)

    case class BooleanSchema(override val schema: Schema)
      extends Primitive(Tag.BooleanTag):
      override def serializeToJType(data: Boolean): java.lang.Boolean                    = data
      override def deserializeFromJType(obj: java.lang.Boolean): Either[String, Boolean] = Right(obj)

    case class IntSchema(override val schema: Schema) extends Primitive(Tag.IntTag):
      override def serializeToJType(data: Int): java.lang.Integer          = data
      override def deserializeFromJType(obj: Integer): Either[String, Int] = Right(obj)

    case class LongSchema(override val schema: Schema) extends Primitive(Tag.LongTag):
      override def serializeToJType(data: Long): JType                    = data
      override def deserializeFromJType(obj: JType): Either[String, Long] = Right(obj)

    case class FloatSchema(override val schema: Schema) extends Primitive(Tag.FloatTag):
      override def serializeToJType(data: Float): JType                    = data
      override def deserializeFromJType(obj: JType): Either[String, Float] = Right(obj)

    case class DoubleSchema(override val schema: Schema) extends Primitive(Tag.DoubleTag):
      override def serializeToJType(data: Double): JType                    = data
      override def deserializeFromJType(obj: JType): Either[String, Double] = Right(obj)

    case class ArrayByteSchema(override val schema: Schema) extends Primitive(Tag.ArrayBytesTag):
      override def serializeToJType(data: Array[Byte]): JType                    = ByteBuffer.wrap(data)
      override def deserializeFromJType(obj: JType): Either[String, Array[Byte]] = Right(obj.array())

    case class StringSchema(override val schema: Schema) extends Primitive(Tag.StringTag):
      override def serializeToJType(data: String): JType                    = org.apache.avro.util.Utf8(data)
      override def deserializeFromJType(obj: JType): Either[String, String] = Right(obj.toString)

  case class Field[Name <: String & Singleton, T](
      name: Name,
      schema: KSchema[T],
      default: Option[T] = None
  )

  case class RecordSchema[Name <: String & Singleton, T <: AnyNamedTuple](
      fields: List[Field[?, ?]],
      schema: Schema
  )(using ValueOf[Name]) extends NamedSchema[Name, T]:
    override type JType = GenericRecord
    override val JTypeClassName: String                = s"GenericRecord(name = ${name})"
    override def isJType(value: AnyRef): Option[JType] =
      value match
        case d: GenericRecord if (d.getSchema().getFullName() == name) => Some(d)
        case _                                                         => None

    override def serializeToJType(data: Name ~ T): GenericRecord =
      val record = new GenericData.Record(schema)
      val values = NamedTuple.toList(data.value.asInstanceOf)
      values.lazyZip(fields).foreach { case (value, field: Field[?, t]) =>
        record.put(
          field.name,
          field.schema.serializeToJType(value.asInstanceOf[t])
        )
      }
      record

    override def deserializeFromJType(
        obj: GenericRecord
    ): Either[String, Name ~ T] =
      val (errs, succ) = fields.partitionMap { f =>
        val v = obj.get(f.name)
        f.schema.isJType(v) match
          case Some(j) => f.schema.deserializeFromJType(j)
          case None    => Left(s"Expected ${f.schema.JTypeClassName}, got: $v")
      }
      Either.cond(
        errs.isEmpty,
        Named.make(Tuple.fromArray(succ.toArray).asInstanceOf[T]),
        errs.mkString(",")
      )

  case class ArraySchema[T: ClassTag](
      override val schema: Schema,
      element: KSchema[T]
  ) extends KSchema[Vector[T]]:
    override type JType = java.util.Collection[?]
    override val JTypeClassName: String                = "java.util.Collection[?]"
    override def isJType(value: AnyRef): Option[JType] =
      value match
        case d: java.util.Collection[?] => Some(d)
        case _                          => None

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
  ) extends KSchema[Map[String, T]]:
    override type JType = java.util.Map[org.apache.avro.util.Utf8, ?]
    override val JTypeClassName: String                = "java.util.Map[org.apache.avro.util.Utf8, ?]"
    override def isJType(value: AnyRef): Option[JType] =
      summon[ClassTag[java.util.Map[org.apache.avro.util.Utf8, ?]]].unapply(value)

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

  case class EnumSchema[Name <: String & Singleton, T <: String](
      override val schema: Schema,
      symbols: ListSet[String]
  )(using ValueOf[Name]) extends NamedSchema[Name, T]:
    override type JType = EnumSymbol
    override val JTypeClassName: String                = s"EnumSymbol(name = ${name})"
    override def isJType(value: AnyRef): Option[JType] =
      value match
        case e: EnumSymbol if (e.getSchema().getFullName() == name) => Some(e)
        case _                                                      => None

    override def serializeToJType(data: Name ~ T): EnumSymbol =
      EnumSymbol(schema, data.value)

    override def deserializeFromJType(obj: EnumSymbol): Either[String, Name ~ T] =
      val str = obj.toString
      Right(Named.make(str.asInstanceOf[T]))

  case class FixedSchema[Name <: String & Singleton, T <: Int & Singleton](
      override val schema: Schema,
      size: Int
  )(using ValueOf[T], ValueOf[Name]) extends NamedSchema[Name, BytesN[T]]:
    override type JType = GenericFixed
    override val JTypeClassName: String                = s"GenericFixed(name = ${name})"
    override def isJType(value: AnyRef): Option[JType] =
      value match
        case d: GenericFixed if (d.getSchema().getFullName() == name) => Some(d)
        case _                                                        => None

    override def serializeToJType(data: Name ~ BytesN[T]): GenericFixed =
      new SpecificFixed:
        self =>
        override val bytes: Array[Byte]                  = data.value.bytes
        override def getSchema(): org.apache.avro.Schema = schema

        val writer: DatumWriter[AnyRef]                             = new SpecificDatumWriter[AnyRef](schema)
        override def writeExternal(out: java.io.ObjectOutput): Unit =
          writer.write(self, SpecificData.getEncoder(out))

        val reader: DatumReader[AnyRef]                 = new SpecificDatumReader[AnyRef](schema)
        def readExternal(in: java.io.ObjectInput): Unit =
          reader.read(self, SpecificData.getDecoder(in))

    override def deserializeFromJType(
        obj: GenericFixed
    ): Either[String, Name ~ BytesN[T]] =
      val arr = obj.bytes()
      BytesN.from[T](
        arr
      ).toRight(s"Expected array of bytes with size $size, got: size ${arr.length}").map(Named.make(_))

  case class UnionSchema[T](
      override val schema: Schema,
      variants: SeqMap[Tag, KSchema[?]]
  ) extends KSchema[T]:
    override type JType = AnyRef

    override val JTypeClassName: String = variants.map((k, _) => k).mkString("|")

    override def isJType(value: AnyRef): Option[AnyRef] = Some(value)

    override def serializeToJType(data: T): AnyRef =
      Tag.fromScala(data).flatMap { tv =>
        variants.get(tv.t).map { case (s: KSchema[typ]) => s.serializeToJType(data.asInstanceOf[typ]) }
      }.getOrElse(throw new Exception("shouldn't be thrown"))

    override def deserializeFromJType(obj: AnyRef): Either[String, T] =
      Tag.fromJava(obj)
        .toRight(s"Unable to get tag for $obj: ${obj.getClass()}").flatMap { tv =>
          variants.get(tv.t)
            .toRight(s"Unable to deserialize $obj, deserializer not found").flatMap {
              case (s: KSchema[typ]) =>
                s.deserializeFromJType(tv.v.asInstanceOf[s.JType]).asInstanceOf[Either[String, T]]
            }
        }

  private def serializeUnsafe(obj: AnyRef, schema: Schema): Array[Byte] = {
    val writer  = new GenericDatumWriter[AnyRef](schema)
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

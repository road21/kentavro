package kentavro.internal

import scala.NamedTuple.{AnyNamedTuple, NamedTuple}
import scala.quoted.*
import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import java.util.List as JavaList
import scala.jdk.CollectionConverters.*
import kentavro.KSchema

private[kentavro] object MacroUtils:
  /**
    * Helper class to build KSchema.Record inside macro
    */
  case class RecordBuilder[T <: AnyNamedTuple](
      fields: List[KSchema.Field[?, ?]],
      default: Option[T]
  ):
    def build(schema: Schema): KSchema[T] =
      KSchema.Record[T](fields, default, schema)

  /**
    * Parse list of avro fields into Expr[RecordBuilder[?]]
    * 
    * @return tuple of (
    *   names tuple type, 
    *   values tuple type,
    *   Expr[RecordBuilder] - partially build record
    * )
    */
  def parseFields(fields: List[Field])(using
      quotes: Quotes
  ): (quotes.reflect.TypeRepr, quotes.reflect.TypeRepr, Expr[RecordBuilder[?]]) =
    import quotes.reflect.*

    fields match
      case h :: tail =>
        if (h.schema() == null)
          quotes.reflect.report.errorAndAbort("null!!!")

        val (htype, hinst)          = parseSchema(h.schema())
        val (tnames, ttypes, tinst) = parseFields(tail)

        (Expr(h.name()), htype.asType, tnames.asType, ttypes.asType) match
          case (
                '{ type hname <: String & scala.Singleton; $l: hname },
                '[htype],
                '[type names <: Tuple; names],
                '[type types <: Tuple; types]
              ) =>
            (
              TypeRepr.of[hname *: names],
              TypeRepr.of[htype *: types],
              ('{
                RecordBuilder[NamedTuple[hname *: names, htype *: types]](
                  KSchema.Field[hname, htype](${ l }, ${ hinst.asInstanceOf[Expr[KSchema[htype]]] }) :: $tinst.fields,
                  None // TODO: support default values
                )
              })
            )
          case _ =>
            quotes.reflect.report.errorAndAbort("unexpected error")
      case _ =>
        (
          TypeRepr.of[EmptyTuple],
          TypeRepr.of[EmptyTuple],
          '{ RecordBuilder[NamedTuple.Empty](Nil, None) }
        )

  /**
    * Parse avro schema into Expr[KSchema[?]]
    * 
    * @return tuple of (
    *   type of schema (named tuple for records, builtin types for primitives),
    *   Expr[KSchema[?]]
    * )
    */
  def parseSchema(schema: Schema)(using quotes: Quotes): (quotes.reflect.TypeRepr, Expr[KSchema[?]]) = {
    import quotes.reflect.*

    schema.getType() match
      case Schema.Type.RECORD =>
        val fields                       = schema.getFields().asScala.toList
        val (namesRepr, typesRepr, inst) = parseFields(fields)
        (namesRepr.asType, typesRepr.asType) match
          case ('[type names <: Tuple; names], '[type types <: Tuple; types]) =>
            (TypeRepr.of[NamedTuple[names, types]], '{ ${ inst }.build(${ Expr(schema) }) })
          case _ =>
            quotes.reflect.report.errorAndAbort("unexpected error")
      case Schema.Type.NULL =>
        (TypeRepr.of[Null], '{ KSchema.Primitive.NullSchema(${ Expr(schema) }) })
      case Schema.Type.BOOLEAN =>
        (TypeRepr.of[Boolean], '{ KSchema.Primitive.BooleanSchema(${ Expr(schema) }) })
      case Schema.Type.INT =>
        (TypeRepr.of[Int], '{ KSchema.Primitive.IntSchema(${ Expr(schema) }) })
      case Schema.Type.LONG =>
        (TypeRepr.of[Long], '{ KSchema.Primitive.LongSchema(${ Expr(schema) }) })
      case Schema.Type.FLOAT =>
        (TypeRepr.of[Float], '{ KSchema.Primitive.FloatSchema(${ Expr(schema) }) })
      case Schema.Type.DOUBLE =>
        (TypeRepr.of[Double], '{ KSchema.Primitive.DoubleSchema(${ Expr(schema) }) })
      case Schema.Type.BYTES =>
        (TypeRepr.of[Array[Byte]], '{ KSchema.Primitive.ArrayByteSchema(${ Expr(schema) }) })
      case Schema.Type.STRING =>
        (TypeRepr.of[String], '{ KSchema.Primitive.StringSchema(${ Expr(schema) }) })
      case _ =>
        quotes.reflect.report.errorAndAbort(
          "not implemented yet" // TODO: support all avro types
        )
  }

  given [A: ToExpr: Type] => ToExpr[JavaList[A]]:
    def apply(x: JavaList[A])(using Quotes): Expr[JavaList[A]] =
      if (x.isEmpty())
        '{ JavaList.of[A]() }
      else
        val exprs: Seq[Expr[A]] = x.asScala.toIndexedSeq.map(Expr.apply[A])
        '{ JavaList.of[A](${ Varargs(exprs) }*) }

  given ToExpr[Field]:
    def apply(x: Field)(using Quotes): Expr[Field] =
      val doc = Option(x.doc()).map(Expr.apply).getOrElse('{ null })
      '{ Field(${ Expr(x.name()) }, ${ Expr(x.schema()) }, $doc) }

  given ToExpr[Schema]:
    def apply(x: Schema)(using Quotes): Expr[Schema] =
      x.getType() match
        case Schema.Type.NULL =>
          '{ Schema.create(Schema.Type.NULL) }
        case Schema.Type.BOOLEAN =>
          '{ Schema.create(Schema.Type.BOOLEAN) }
        case Schema.Type.INT =>
          '{ Schema.create(Schema.Type.INT) }
        case Schema.Type.LONG =>
          '{ Schema.create(Schema.Type.LONG) }
        case Schema.Type.FLOAT =>
          '{ Schema.create(Schema.Type.FLOAT) }
        case Schema.Type.DOUBLE =>
          '{ Schema.create(Schema.Type.DOUBLE) }
        case Schema.Type.BYTES =>
          '{ Schema.create(Schema.Type.BYTES) }
        case Schema.Type.STRING =>
          '{ Schema.create(Schema.Type.STRING) }
        case Schema.Type.RECORD =>
          val name = Option(x.getName()).map(Expr.apply).getOrElse('{ null })
          val doc  = Option(x.getDoc()).map(Expr.apply).getOrElse('{ null })
          val ns   = Option(x.getNamespace()).map(Expr.apply).getOrElse('{ null })

          '{
            Schema.createRecord(
              $name,
              $doc,
              $ns,
              ${ Expr(x.isError()) },
              ${ Expr(x.getFields()) }
            )
          }
        case typ => // TODO: support all avro types
          quotes.reflect.report.errorAndAbort(
            s"type ${typ} is not supported yet"
          )

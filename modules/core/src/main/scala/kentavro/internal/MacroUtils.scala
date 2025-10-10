package kentavro.internal

import scala.NamedTuple.{AnyNamedTuple, NamedTuple}
import scala.quoted.*
import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import java.util.List as JavaList
import scala.jdk.CollectionConverters.*
import scala.reflect.ClassTag
import kentavro.{AvroType, BytesN, KSchema}
import scala.collection.immutable.ListSet
import kentavro.~
import kentavro.AvroType.{ArraySchema, MapSchema, NamedSchema, Primitive, Tag, UnionSchema}
import scala.collection.immutable.ListMap
import kentavro.NamedCompanion
import scala.annotation.experimental

private[kentavro] object MacroUtils:
  /**
    * Helper class to build KSchema.Record inside macro
    */
  case class RecordBuilder[T <: AnyNamedTuple](
      fields: List[AvroType.Field[?, ?]]
  ):
    def build[Name <: String & Singleton](name: Name, schema: Schema): AvroType[Name ~ T] =
      AvroType.RecordSchema[Name, T](fields, schema)(using ValueOf(name))

  class CompanionDecl[Q <: Quotes](using val q: Q)(
      val decl: (cls: q.reflect.Symbol) => q.reflect.Symbol,
      val inst: Expr[NamedCompanion[?, ?]]
  )

  class Members[Q <: Quotes](using val q: Q)(
      val typeMembers: Map[String, (cls: q.reflect.Symbol) => q.reflect.Symbol] = Map.empty,
      val decls: Map[String, CompanionDecl[q.type]] = Map.empty,
  ):
    def ++(other: Members[q.type]): Members[q.type] =
      Members(typeMembers ++ other.typeMembers, decls ++ other.decls)

    def +(name: String, decl: CompanionDecl[q.type]): Members[q.type] =
      Members(typeMembers, decls + (name -> decl))

    def allDecls(cls: q.reflect.Symbol): List[q.reflect.Symbol] =
      (typeMembers.values ++ decls.values.map(_.decl)).map(x => x(cls)).toList

  object Members:
    def empty[Q <: Quotes](using q: Q): Members[Q] = new Members(Map.empty, Map.empty)

  class FieldsParsed[Q <: Quotes](using val q: Q)(
      val names: q.reflect.TypeRepr,
      val fieldTypes: q.reflect.TypeRepr,
      val inst: (cls: Quotes) ?=> Expr[RecordBuilder[?]],
      val members: Members[q.type] = Members.empty
  )

  class AvroTypeParsed[Q <: Quotes](using val q: Q)(
      val typeArg: q.reflect.TypeRepr,
      val inst: (cls: Quotes) ?=> Expr[AvroType[?]],
      val members: Members[q.type] = Members.empty
  )

  /**
    * Parse list of avro fields into Expr[RecordBuilder[?]]
    * 
    * @return tuple of (
    *   names tuple type, 
    *   values tuple type,
    *   Expr[RecordBuilder] - partially build record
    * )
    */
  def parseFields[Q <: Quotes](fields: List[Field])(using
      quotes: Q
  ): FieldsParsed[quotes.type] =
    import quotes.reflect.*

    fields match
      case h :: tail =>
        val avroTypeParsed = parseAvroType(h.schema())
        val htype          = avroTypeParsed.typeArg
        val hinst          = avroTypeParsed.inst

        val fieldsParsed = parseFields(tail)
        val tnames       = fieldsParsed.names
        val ttypes       = fieldsParsed.fieldTypes
        val tinst        = fieldsParsed.inst

        (Expr(h.name()), htype.asType, tnames.asType, ttypes.asType) match
          case (
                '{ type hname <: String & scala.Singleton; $l: hname },
                '[htype],
                '[type names <: Tuple; names],
                '[type types <: Tuple; types]
              ) =>
            FieldsParsed(
              TypeRepr.of[hname *: names],
              TypeRepr.of[htype *: types],
              ('{
                RecordBuilder[NamedTuple[hname *: names, htype *: types]](
                  AvroType.Field[hname, htype](${ l }, ${ hinst.asInstanceOf[Expr[AvroType[htype]]] }) :: $tinst.fields,
                )
              }),
              avroTypeParsed.members ++ fieldsParsed.members
            )
          case _ =>
            quotes.reflect.report.errorAndAbort("unexpected error")
      case _ =>
        FieldsParsed(
          TypeRepr.of[EmptyTuple],
          TypeRepr.of[EmptyTuple],
          '{ RecordBuilder[NamedTuple.Empty](Nil) },
          Members.empty
        )

  def unionOf(using
      Quotes
  )(fst: quotes.reflect.TypeRepr, other: List[quotes.reflect.TypeRepr]): quotes.reflect.TypeRepr =
    import quotes.reflect.*
    other match
      case head :: next =>
        OrType(fst, unionOf(head, next))
      case Nil => fst

  /**
    * Parse avro schema into Expr[KSchema[?]]
    * 
    * @return tuple of (
    *   type of schema (named tuple for records, builtin types for primitives),
    *   Expr[KSchema[?]]
    * )
    */
  def parseAvroType[Q <: Quotes](schema: Schema)(using quotes: Q): AvroTypeParsed[quotes.type] =
    import quotes.reflect.*

    schema.getType() match
      case Schema.Type.NULL =>
        AvroTypeParsed(TypeRepr.of[Null], '{ AvroType.Primitive.NullSchema(${ Expr(schema) }) })
      case Schema.Type.BOOLEAN =>
        AvroTypeParsed(TypeRepr.of[Boolean], '{ AvroType.Primitive.BooleanSchema(${ Expr(schema) }) })
      case Schema.Type.INT =>
        AvroTypeParsed(TypeRepr.of[Int], '{ AvroType.Primitive.IntSchema(${ Expr(schema) }) })
      case Schema.Type.LONG =>
        AvroTypeParsed(TypeRepr.of[Long], '{ AvroType.Primitive.LongSchema(${ Expr(schema) }) })
      case Schema.Type.FLOAT =>
        AvroTypeParsed(TypeRepr.of[Float], '{ AvroType.Primitive.FloatSchema(${ Expr(schema) }) })
      case Schema.Type.DOUBLE =>
        AvroTypeParsed(TypeRepr.of[Double], '{ AvroType.Primitive.DoubleSchema(${ Expr(schema) }) })
      case Schema.Type.BYTES =>
        AvroTypeParsed(TypeRepr.of[Array[Byte]], '{ AvroType.Primitive.ArrayByteSchema(${ Expr(schema) }) })
      case Schema.Type.STRING =>
        AvroTypeParsed(TypeRepr.of[String], '{ AvroType.Primitive.StringSchema(${ Expr(schema) }) })
      case Schema.Type.ARRAY =>
        val elemSchema     = schema.getElementType()
        val parsedAvroType = parseAvroType(elemSchema)
        val repr           = parsedAvroType.typeArg
        val sch            = parsedAvroType.inst
        repr.asType match
          case '[t] =>
            val ct = Expr.summon[ClassTag[t]]
            ct match
              case Some(ctInst) =>
                AvroTypeParsed(
                  TypeRepr.of[Vector[t]],
                  '{ AvroType.ArraySchema[t](${ Expr(schema) }, ${ sch }.asInstanceOf[AvroType[t]])(using $ctInst) },
                  parsedAvroType.members
                )
              case None =>
                quotes.reflect.report.errorAndAbort(
                  s"no class tag found for ${repr.show}"
                )
      case Schema.Type.MAP =>
        val valueSchema = schema.getValueType()
        val parsed      = parseAvroType(valueSchema)
        parsed.typeArg.asType match
          case '[t] =>
            AvroTypeParsed(
              TypeRepr.of[Map[String, t]],
              '{ AvroType.MapSchema[t](${ Expr(schema) }, ${ parsed.inst }.asInstanceOf[AvroType[t]]) },
              parsed.members
            )
      case Schema.Type.UNION =>
        val parsed = schema.getTypes().asScala.toList.map(s => parseAvroType[quotes.type](s))
        val types: List[quotes.reflect.TypeRepr] = parsed.map(_.typeArg)
        val insts                                = Expr.ofList(parsed.map(_.inst))
        types match
          case h :: t =>
            val resultType = unionOf(h, t)
            resultType.asType match
              case '[t] =>
                AvroTypeParsed(
                  resultType,
                  '{
                    AvroType.UnionSchema[t](
                      ${ Expr(schema) },
                      ListMap.from(${ insts }.map {
                        case s: Primitive[?, ?]   => s.tag                -> s
                        case s: NamedSchema[?, ?] => Tag.NamedTag(s.name) -> s
                        case s: ArraySchema[?]    => Tag.ArrayTag         -> s
                        case s: MapSchema[?]      => Tag.MapTag           -> s
                        case s: UnionSchema[?]    => throw new Exception("shouldn't be thrown")
                      })
                    )
                  },
                  parsed.map(_.members).foldLeft(Members.empty[quotes.type])(_ ++ _)
                )
              case _ =>
                quotes.reflect.report.errorAndAbort("unexpected error")
          case _ =>
            quotes.reflect.report.errorAndAbort("union can't be empty")
      case Schema.Type.RECORD =>
        val fields = schema.getFields().asScala.toList

        val parsed: FieldsParsed[quotes.type] = parseFields(fields)
        val namesRepr                         = parsed.names
        val typesRepr                         = parsed.fieldTypes
        val inst                              = parsed.inst

        val fullName = schema.getFullName()
        (Expr(fullName), namesRepr.asType, typesRepr.asType) match
          case (
                '{ type schemaName <: String & scala.Singleton; $schemaName: schemaName },
                '[type names <: Tuple; names],
                '[type types <: Tuple; types]
              ) =>
            val declInst: Expr[NamedCompanion[?, ?]] =
              '{ new NamedCompanion.Record[schemaName, NamedTuple[names, types]](${ schemaName }) }
            val decl = CompanionDecl[quotes.type](
              cls =>
                Symbol.newVal(
                  cls,
                  fullName,
                  TypeRepr.of[NamedCompanion.Record[schemaName, NamedTuple[names, types]]],
                  Flags.EmptyFlags,
                  Symbol.noSymbol
                ),
              declInst
            )
            AvroTypeParsed[quotes.type](
              TypeRepr.of[schemaName ~ NamedTuple[names, types]],
              '{ ${ inst }.build[schemaName](${ schemaName }, ${ Expr(schema) }) },
              parsed.members + (fullName, decl)
            )
          case _ =>
            quotes.reflect.report.errorAndAbort("unexpected error")
      case Schema.Type.ENUM =>
        val symbols = schema.getEnumSymbols().asScala.toList
        symbols match
          case h :: t =>
            val typ = makeUnionTypeOfLiterals(h, t)
            (Expr(schema.getFullName()), typ.asType) match
              case (
                    '{ type schemaName <: String & scala.Singleton; $schemaName: schemaName },
                    '[type typ <: String; typ]
                  ) =>
                AvroTypeParsed(
                  TypeRepr.of[schemaName ~ typ],
                  '{
                    AvroType.EnumSchema[schemaName, typ](${ Expr(schema) }, ListSet.from(${ Expr(symbols) }))(using
                      ValueOf(${ schemaName })
                    )
                  },
                  Members.empty // TODO
                )
              case _ => quotes.reflect.report.errorAndAbort("unexpected error")
          case _ =>
            quotes.reflect.report.errorAndAbort("enums should contain at least one symbol")
      case Schema.Type.FIXED =>
        val size = schema.getFixedSize()
        (Expr(schema.getFullName()), Expr(size)) match
          case (
                '{ type schemaName <: String & scala.Singleton; $schemaName: schemaName },
                '{ type s <: Int & scala.Singleton; $s: s }
              ) =>

            AvroTypeParsed(
              TypeRepr.of[schemaName ~ BytesN[s]],
              '{
                AvroType.FixedSchema[schemaName, s](${ Expr(schema) }, ${ Expr(size) })(using
                  ValueOf(${ s }),
                  ValueOf(${ schemaName })
                )
              },
              Members.empty // TODO
            )
          case _ =>
            quotes.reflect.report.errorAndAbort("unexpected error")

  @experimental
  def parseSchema(schema: Schema)(using Quotes): Expr[KSchema[?]] =
    import quotes.reflect.*
    val parsed       = parseAvroType(schema)
    val name: String = "Impl"

    parsed.typeArg.asType match
      case '[typeArg] =>
        val parents = List(TypeTree.of[Object], TypeTree.of[KSchema[typeArg]])

        def kSchemaOverride(cls: quotes.reflect.Symbol): List[quotes.reflect.Symbol] =
          List(
            Symbol.newVal(cls, "avroType", TypeRepr.of[AvroType[typeArg]], Flags.Override, Symbol.noSymbol),
            Symbol.newVal(cls, "values", TypeRepr.of[Map[String, Any]], Flags.Override, Symbol.noSymbol)
          )

        val cls = Symbol.newClass(
          Symbol.spliceOwner,
          name,
          parents = parents.map(_.tpe),
          cls => kSchemaOverride(cls) ++ parsed.members.allDecls(cls),
          selfType = None
        )

        val tpeVal    = cls.declaredField("avroType")
        val tpeValDef = {
          given q2: Quotes = tpeVal.asQuotes
          ValDef(tpeVal, Some(parsed.inst.asTerm))
        }

        val valuesVal = cls.declaredField("values")

        val valuesExpr = '{
          Map.from(
            ${
              Expr.ofSeq(parsed.members.decls.map((key, inst) =>
                val replaced = key.replace(".", "$u002E") // TODO: make it for all possibly escaped letters
                '{ ${ Expr(replaced) } -> ${ inst.inst } }
              ).toSeq)
            }
          )
        }

        val valuesValDef = {
          given q2: Quotes = valuesVal.asQuotes
          ValDef(valuesVal, Some(valuesExpr.asTerm))
        }

        val valDefs = tpeValDef :: valuesValDef :: parsed.members.decls.map { (name, decl) =>
          val method = cls.declaredField(name)
          ValDef(method, Some(decl.inst.asTerm))
        }.toList

        val clsDef = ClassDef(cls, parents, body = valDefs)
        val newCls = Apply(Select(New(TypeIdent(cls)), cls.primaryConstructor), Nil)
        Block(List(clsDef), newCls).asExprOf[KSchema[?]]

  def makeUnionTypeOfLiterals(head: String, tail: List[String])(using Quotes): quotes.reflect.TypeRepr =
    import quotes.reflect.*

    val repr = Expr(head) match
      case '{ type h <: String & scala.Singleton; $h: h } =>
        TypeRepr.of[h]
      case _ =>
        quotes.reflect.report.errorAndAbort("unexpected error")

    tail match
      case htail :: ttail =>
        OrType(repr, makeUnionTypeOfLiterals(htail, ttail))
      case Nil => repr

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
        case Schema.Type.ARRAY =>
          '{ Schema.createArray(${ Expr(x.getElementType()) }) }
        case Schema.Type.FIXED =>
          val name = Expr(x.getName())
          val ns   = Option(x.getNamespace()).map(Expr.apply).getOrElse('{ null })
          val doc  = Option(x.getDoc()).map(Expr.apply).getOrElse('{ null })
          val size = Expr(x.getFixedSize())

          '{ Schema.createFixed($name, $doc, $ns, $size) }
        case Schema.Type.ENUM =>
          val name    = Expr(x.getName())
          val doc     = Option(x.getDoc()).map(Expr.apply).getOrElse('{ null })
          val ns      = Option(x.getNamespace()).map(Expr.apply).getOrElse('{ null })
          val symbols = Expr(x.getEnumSymbols())
          val default = Option(x.getEnumDefault()).map(Expr.apply).getOrElse('{ null })

          '{ Schema.createEnum($name, $doc, $ns, $symbols, $default) }
        case Schema.Type.MAP =>
          val valueSchema = x.getValueType()
          '{ Schema.createMap(${ Expr(valueSchema) }) }
        case Schema.Type.RECORD =>
          val name = Expr(x.getName())
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
        case Schema.Type.UNION =>
          '{ Schema.createUnion(${ Expr(x.getTypes()) }) }

package kentavro

import scala.io.Source
import scala.quoted.*
import scala.util.{Failure, Success, Try}
import org.apache.avro.Schema
import kentavro.internal.MacroUtils
import scala.annotation.experimental

object Avsc:
  /**
    * Parse avsc schema to {@link kentavro.KSchema KSchema}.
    * The type of result schema will be refined in compile-time.
    * 
    * Example:
    * <pre><snippet>
    *   Avsc.fromString("""{ "type": "string" }"""): KSchema[String]
    *   Avsc.fromString(
    *     """{ "type": "record", "name": "User", "namespace": "ns",
    *       | "fields": [
    *       |  { "name": "name", "type": "string" },
    *       |  { "name": "age", "type": "int" }
    *       |]}
    *     """.stripMargin
    *  ): KSchema[(name: String, age: Int)]
    * </snippet></pre>
    *
    * @param schema avsc schema (json) in string literal value
    * @return schema
    */
  @experimental
  transparent inline def fromString(
      inline schema: String
  ): KSchema[?] =
    ${ AvscImpl.fromString('schema) }

  @experimental
  transparent inline def fromStringType(
      inline schema: String
  ): AvroType[?] =
    ${ AvscImpl.fromStringType('schema) }

  /**
    * Parse avsc schema from file to {@link kentavro.KSchema KSchema}.
    * The file will be read in compile-time and parsed into {@link kentavro.KSchema KSchema}.
    *
    * Example:
    * <pre><snippet>
    *   // file "/path/to/file.json" contains `{ "type": "string" }`
    *   Avsc.fromFile("/path/to/file.json"): KSchema[String]
    * </snippet></pre>
    *
    * @param schema - path to file contains avsc schema (json)
    * @return schema
    *
    */
  @experimental
  transparent inline def fromFile(
      inline path: String
  ): KSchema[?] =
    ${ AvscImpl.fromFile('path) }

private[kentavro] object AvscImpl:
  @experimental
  def parseString(string: String)(using
      Quotes
  ): Expr[KSchema[?]] =
    Try(
      new Schema.Parser().parse(string)
    ) match
      case Success(res) =>
        MacroUtils.parseSchema(res)
      case Failure(ex) =>
        quotes.reflect.report.errorAndAbort(
          "Unable to parse schema: " + ex.getMessage()
        )

  @experimental
  def fromFile(path: Expr[String])(using Quotes): Expr[KSchema[?]] =
    path.value match
      case Some(fileName) =>
        Try(Source.fromFile(fileName).mkString).fold(
          ex => quotes.reflect.report.errorAndAbort("Unable to read file: " + ex.getMessage()),
          parseString
        )
      case _ =>
        quotes.reflect.report.errorAndAbort(
          "expected string literal value (path to file)"
        )

  @experimental
  def fromString(schema: Expr[String])(using Quotes): Expr[KSchema[?]] =
    schema.value match
      case Some(value) =>
        parseString(value)
      case _ =>
        quotes.reflect.report.errorAndAbort("expected string literal value (avsc schema)")

  @experimental
  def fromStringType(schema: Expr[String])(using Quotes): Expr[AvroType[?]] =
    import quotes.reflect.*
    schema.value match
      case Some(value) =>
        scala.util.Try(
          new Schema.Parser().parse(value)
        ) match
          case Success(res) =>
            // Symbol.newVal()
            // val vdef = ValDef("")
            MacroUtils.parseAvroType(res).inst.asTerm.asExprOf[AvroType[?]]
          case Failure(ex) =>
            quotes.reflect.report.errorAndAbort(
              "Unable to parse schema: " + ex.getMessage()
            )
      case _ =>
        quotes.reflect.report.errorAndAbort("expected string literal value (avsc schema)")

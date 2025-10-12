package kentavro

import scala.quoted.*
import scala.quoted.quotes
import scala.util.{Failure, Success, Try, Using}
import kentavro.internal.MacroUtils
import org.apache.avro.idl.IdlReader
import java.nio.file.Paths
import java.nio.file.Path
import java.io.ByteArrayInputStream
import scala.annotation.experimental

object Avdl:
  def withImports[P <: String & Singleton](path: P): AvdlWithImports[P] =
    new AvdlWithImports[P](path)

  @experimental
  transparent inline def fromString(
      inline avdl: String
  ): KSchema[?] =
    ${ AvdlImpl.fromString('avdl) }

  @experimental
  transparent inline def fromFile(
      inline filePath: String
  ): KSchema[?] =
    ${ AvdlImpl.fromFileSingle('filePath) }

  case class AvdlWithImports[Dir <: String & Singleton](dir: Dir) extends AnyVal:
    @experimental
    transparent inline def fromFileIn(
        inline relativePath: String
    ): KSchema[?] =
      ${ AvdlImpl.fromFileIn[Dir]('relativePath) }

    @experimental
    transparent inline def fromFileOut(
        inline absolutePath: String
    ): KSchema[?] =
      ${ AvdlImpl.fromFileIn[Dir]('absolutePath) }

private[kentavro] object AvdlImpl:
  def readFile(path: Path)(using Quotes): String =
    Try { java.nio.file.Files.readString(path) } match
      case Success(str) =>
        str
      case Failure(e) =>
        quotes.reflect.report.errorAndAbort(s"Failed to read data from file '$path': $e")

  @experimental
  def parseSchema(
      schema: String,
      directory: Path
  )(using Quotes): Expr[KSchema[?]] =
    Try {
      new IdlReader().parse(
        directory.toUri(),
        schema
      ).getMainSchema()
    } match
      case Success(res) if res != null =>
        MacroUtils.parseSchema(res)
      case Success(_) =>
        quotes.reflect.report.errorAndAbort(
          s"No main schema in detected"
        )
      case Failure(ex) =>
        quotes.reflect.report.errorAndAbort(
          "Unable to parse schema: " + ex.getMessage()
        )

  @experimental
  def fromString(schema: Expr[String])(using Quotes): Expr[KSchema[?]] =
    schema.value match
      case Some(v) =>
        Using(
          new ByteArrayInputStream(v.getBytes())
        )(stream =>
          new IdlReader().parse(stream).getMainSchema()
        ) match
          case Success(res) if res != null =>
            MacroUtils.parseSchema(res)
          case Success(_) =>
            quotes.reflect.report.errorAndAbort(
              s"No main schema in detected"
            )
          case Failure(ex) =>
            quotes.reflect.report.errorAndAbort(
              "Unable to parse schema: " + ex.getMessage()
            )
      case _ => quotes.reflect.report.errorAndAbort(
          "expected string literal argument"
        )

  @experimental
  def fromFileSingle(filePath: Expr[String])(using Quotes): Expr[KSchema[?]] =
    filePath.value match
      case Some(v) =>
        Try(
          new IdlReader().parse(Paths.get(v)).getMainSchema()
        ) match
          case Success(res) if res != null =>
            MacroUtils.parseSchema(res)
          case Success(_) =>
            quotes.reflect.report.errorAndAbort(
              s"No main schema in detected"
            )
          case Failure(ex) =>
            quotes.reflect.report.errorAndAbort(
              "Unable to parse schema: " + ex.getMessage()
            )
      case _ => quotes.reflect.report.errorAndAbort(
          "expected string literal argument"
        )

  @experimental
  def fromFileIn[Dir <: Singleton & String: Type](fileName: Expr[String])(using Quotes): Expr[KSchema[?]] =
    (fileName.value, Type.valueOfConstant[Dir]) match
      case (Some(file), Some(dir)) =>
        val dirPath = Paths.get(dir)
        val schema  = readFile(dirPath.resolve(file))
        parseSchema(schema, dirPath)
      case _ =>
        quotes.reflect.report.errorAndAbort(
          "expected string literals arguments"
        )

  @experimental
  def fromFileOut[Dir <: Singleton & String: Type](fileName: Expr[String])(using
      Quotes
  ): Expr[KSchema[?]] =
    (fileName.value, Type.valueOfConstant[Dir]) match
      case (Some(file), Some(dir)) =>
        val dirPath = Paths.get(dir)
        val schema  = readFile(Paths.get(file))
        parseSchema(schema, dirPath)
      case _ =>
        quotes.reflect.report.errorAndAbort(
          "expected string literals arguments"
        )

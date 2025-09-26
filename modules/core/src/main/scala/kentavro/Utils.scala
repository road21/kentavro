package kentavro

import scala.quoted.*
import scala.NamedTuple.{AnyNamedTuple, DropNames, NamedTuple, Names}
import scala.collection.SeqMap
import scala.collection.immutable.ListMap

object Utils:
  extension [S <: String & Singleton](s: S)
    transparent inline def stripMarginCT: String =
      ${ UtilsImpl.stripMarginCT('s) }

    transparent inline def stripMarginCT(inline marginChar: Char): String =
      ${ UtilsImpl.stripMarginCT('s, 'marginChar) }

  inline def toSeqMap[T <: AnyNamedTuple](
      x: T
  ): SeqMap[String, Any] = {
    inline compiletime.constValueTuple[Names[T]].toList match
      case names: List[String] =>
        ListMap.from(
          names.iterator.zip(
            x.asInstanceOf[NamedTuple[Names[T], DropNames[T]]]
              .toTuple
              .productIterator
              .asInstanceOf[Iterator[Any]]
          )
        )
  }

private[kentavro] object UtilsImpl:
  def stripMarginCT(str: Expr[String])(using Quotes): Expr[String] =
    Expr(str.valueOrAbort.stripMargin)

  def stripMarginCT(str: Expr[String], marginChar: Expr[Char])(using Quotes): Expr[String] =
    Expr(str.valueOrAbort.stripMargin(marginChar.valueOrAbort))

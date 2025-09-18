package kentavro

import scala.quoted.*

object Utils:
  extension [S <: String & Singleton](s: S)
    transparent inline def stripMarginCT: String =
      ${ UtilsImpl.stripMarginCT('s) }

    transparent inline def stripMarginCT(inline marginChar: Char): String =
      ${ UtilsImpl.stripMarginCT('s, 'marginChar) }

private[kentavro] object UtilsImpl:
  def stripMarginCT(str: Expr[String])(using Quotes): Expr[String] =
    Expr(str.valueOrAbort.stripMargin)

  def stripMarginCT(str: Expr[String], marginChar: Expr[Char])(using Quotes): Expr[String] =
    Expr(str.valueOrAbort.stripMargin(marginChar.valueOrAbort))

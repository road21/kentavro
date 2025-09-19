package kentavro.data

import scala.compiletime.constValue

case class Fixed[S <: Int & Singleton] private (bytes: Array[Byte])

object Fixed:
  inline def from[S <: Int & Singleton](array: Array[Byte])(using ValueOf[S]): Option[Fixed[S]] =
    Option.when(array.length == (summon[ValueOf[S]].value))(Fixed(array))

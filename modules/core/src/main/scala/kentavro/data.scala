package kentavro

import scala.NamedTuple.NamedTuple
import scala.NamedTuple.AnyNamedTuple
import scala.compiletime.constValue

/**
  * Representation of named avro types (records, fixed, enums)
  * For example, for fixed type sized 10 named "custom": `"Custom" ~ BytesN[10]`
  */
case class ~[Name <: String & Singleton, +V](val name: Name, val value: V)

object Named:
  def apply[Name <: String & Singleton, V](name: Name, value: V): Name ~ V = new ~(name, value)

  def make[Name <: String & Singleton]: MkNamed[Name] =
    new MkNamed[Name]

  class MkNamed[Name <: String & Singleton](val dummy: Unit = ()):
    def apply[V](v: V)(using name: ValueOf[Name]): Name ~ V =
      new ~[Name, V](name.value, v)

case class BytesN[S <: Int & Singleton] private (bytes: Array[Byte])

object BytesN:
  def from[S <: Int & Singleton](array: Array[Byte])(using ValueOf[S]): Option[BytesN[S]] =
    Option.when(array.length == (summon[ValueOf[S]].value))(BytesN(array))

trait NamedCompanion[Name <: String & Singleton, Base]:
  def name: Name
  type Type = Name ~ Base

object NamedCompanion:
  class Record[Name <: String & Singleton, Base <: AnyNamedTuple](override val name: Name)
    extends NamedCompanion[Name, Base]:
    def apply(fields: Base): (Name ~ Base) =
      Named.make(fields)(using ValueOf(name))

  class Enum[Name <: String & Singleton, V <: Tuple](override val name: Name) extends NamedCompanion[Name, V],
      Selectable:
    type Fields = NamedTuple[V, Tuple.Map[V, [x] =>> Name ~ x]]

    inline def selectDynamic(fld: String): Any =
      Named(constValue[fld.type], fld)

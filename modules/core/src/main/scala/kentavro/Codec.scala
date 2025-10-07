package kentavro

import scala.deriving.Mirror
import scala.NamedTuple.NamedTuple
import scala.compiletime.summonAll

trait AvroWriter[A]:
  def serialize(data: A): Array[Byte]

object AvroWriter:
  def apply[A: AvroWriter as writer]: AvroWriter[A] = writer

  trait Struct[A]:
    type S
    def transformTo(s: A): S

  object Struct:
    trait Tupled[A] extends Struct[A]:
      override type S <: Tuple
    
    object Tupled:
      type Aux[A, _S] = Tupled[A]:
        type S = _S
      
      def instance[A, _S <: Tuple](f: A => _S): Tupled.Aux[A, _S] = new Tupled[A]:
        type S = _S
        def transformTo(a: A): S = f(a)

      given [A](using s: Struct[A]): Tupled.Aux[A *: EmptyTuple, s.S *: EmptyTuple] = 
        instance(a => s.transformTo(a.head) *: EmptyTuple)

      given [A: Struct as as, T <: Tuple: Tupled as ts]: Tupled.Aux[A *: T, as.S *: ts.S] = 
        instance(a => as.transformTo(a.head) *: ts.transformTo(a.tail))

    trait Named[A] extends Struct[A]:
      self =>

      type Name <: String
      type Inner      
      type S = Name ~ Inner

      def transformTo(a: A): S

      def rename[N <: String & Singleton](name: N): Named.Aux[A, N, Inner] = 
        new Named[A]:
          type Name = N
          type Inner = self.Inner
          def transformTo(a: A): N ~ Inner = 
            kentavro.Named(name, self.transformTo(a).value)

    object Named:
      type Aux[A, _Name, _Inner] = Named[A]:
        type Name = _Name
        type Inner = _Inner
      
      def instance[A, _Name <: String & Singleton: ValueOf, _Inner](f: A => _Inner): Named.Aux[A, _Name, _Inner] = 
        new Named[A]:
          type Name = _Name
          type Inner = _Inner
          def transformTo(a: A): _Name ~ _Inner = 
            kentavro.Named.make[_Name](f(a))

    trait Record[A] extends Named[A]:
      self =>

      type Name <: String
      type FieldNames <: Tuple
      type FieldValues <: Tuple

      type Inner = NamedTuple[FieldNames, FieldValues]

      def transformTo(a: A): S

      override def rename[N <: String & Singleton](name: N): Record.Aux[A, N, FieldNames, FieldValues] = 
        new Record[A]:
          type Name = N
          type FieldNames = self.FieldNames
          type FieldValues = self.FieldValues
          def transformTo(a: A): S = 
            kentavro.Named(name, self.transformTo(a).value)


      def renameField[FieldName <: String & Singleton, NewName <: String & Singleton](
        fieldName: FieldName, 
        newName: NewName
      )(using 
        Tuple.Contains[FieldNames, FieldName] =:= true, 
        Tuple.Contains[FieldNames, NewName] =:= false
      ): Record.Aux[A, Name, Record.Rename[FieldNames, FieldName, NewName], FieldValues] = 
        self.asInstanceOf[Record.Aux[A, Name, Record.Rename[FieldNames, FieldName, NewName], FieldValues]]

    object Record:
      type Aux[A, _Name, _FieldNames <: Tuple, _FieldValues <: Tuple] = Record[A]:
        type Name = _Name
        type FieldNames = _FieldNames
        type FieldValues = _FieldValues
      
      type RenameAcc[Ns <: Tuple, FieldName <: String, N <: String, Acc <: Tuple] <: Tuple = 
        Ns match 
          case FieldName *: tail => Tuple.Concat[Tuple.Reverse[Acc], (N *: tail)]
          case f *: tail => RenameAcc[tail, FieldName, N, f *: Acc]
          case EmptyTuple => Ns

      type Rename[Ns <: Tuple, FieldName <: String, N <: String] = RenameAcc[Ns, FieldName, N, EmptyTuple]
 
      def instance[A, _Name <: String: ValueOf, _FieldNames <: Tuple, _FieldValues <: Tuple](
        f: A => _FieldValues
      ): Record.Aux[A, _Name, _FieldNames, _FieldValues] = new Record[A]:
        type Name = _Name
        type FieldNames = _FieldNames
        type FieldValues = _FieldValues

        def transformTo(a: A): _Name ~ NamedTuple[FieldNames, FieldValues] = 
          kentavro.Named.make[_Name](f(a))

    type Aux[A, _S] = Struct[A] { type S = _S }

    def instance[A, _S](f: A => _S): Struct.Aux[A, _S] = new Struct[A]:
      type S = _S
      def transformTo(a: A): S = f(a)

    def identity[A]: Struct.Aux[A, A] = instance(Predef.identity)

    given Struct.Aux[Int, Int] = identity
    given Struct.Aux[String, String] = identity
    given Struct.Aux[Boolean, Boolean] = identity
    given Struct.Aux[Long, Long] = identity
    given Struct.Aux[Float, Float] = identity
    given Struct.Aux[Double, Double] = identity

    // type Res = { type R <: Tuple }
    // def res[_R]: Res { type R = _R } = null // todo: make sure it's ok

    // inline def resultType(t: Tuple, acc: Res): Res =
    //   t match 
    //     case EmptyTuple => res[Tuple.Reverse[acc.R]]
    //     case h *: t => 
    //       val s = h.asInstanceOf[Struct[?]]
    //       res[s.S *: acc.R]


    transparent inline def derived[P <: Product](
      using mirror: Mirror.ProductOf[P],
      ev: Tupled[mirror.MirroredElemTypes],
      valueOf: ValueOf[mirror.MirroredLabel]
    ): Struct.Record.Aux[
      P, 
      mirror.MirroredLabel,
      mirror.MirroredElemLabels,
      ?
    ] =
      Record.instance[
        P, 
        mirror.MirroredLabel, 
        mirror.MirroredElemLabels,
        ev.S
      ] { (a: P) =>
        ???
      }

      
  trait TupleStruct[T <: Tuple]:
    type S <: Tuple

//-----------------------------------------------------------------------------

trait AvroReader[A]:
  def deserialize(bytes: Array[Byte]): Either[String, A]


// trait Codec[A]: // TODO: split to serialize and deserialize
//   type S

//   def struct: Codec.Struct.Aux[A, S]
//   def schema: KSchema[S]

//   final def serialize(data: A): Array[Byte]                    = schema.serialize(struct.to(data))
//   final def deserialize(bytes: Array[Byte]): Either[String, A] = schema.deserialize(bytes).flatMap(struct.from)

// object Codec:
//   trait Struct[A]:
//     type S

//     def to(s: A): S
//     def from(s: S): Either[String, A] // TODO: to think about union instead of either

//     // def bimap[A1](f: A => A1, g: A1 => A): Aux[A1, S] = Struct.instance[A1, S](
//     //   a => to(g(a)),
//     //   s => from(s).flatMap
//     // )

//   object Struct:
//     type Aux[A, _S] = Struct[A] { type S = _S }

//     def instance[A, _S](_to: A => _S, _from: _S => Either[String, A]): Codec.Struct.Aux[A, _S] = new Codec.Struct[A]:
//       type S = _S
//       def to(s: A): S = _to(s)
//       def from(s: _S): Either[String, A] = _from(s)

//     def identity[A]: Struct.Aux[A, A] = new Struct[A]:
//       type S = A
//       def to(s: A): A = s
//       def from(s: A): Either[String, A] = Right(s)

//     given Struct.Aux[Int, Int] = identity
//     given Struct.Aux[String, String] = identity
//     given Struct.Aux[Boolean, Boolean] = identity
//     given Struct.Aux[Long, Long] = identity
//     given Struct.Aux[Float, Float] = identity
//     given Struct.Aux[Double, Double] = identity

    

//   type Aux[A, _S] = Codec[A] { type S = _S }

//   //def derived[S](schema: KSchema[S])
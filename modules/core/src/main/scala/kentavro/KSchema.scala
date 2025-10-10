package kentavro

trait KSchema[T] extends Selectable:
  def tpe: AvroType[T]
  
  def serialize(t: T): Array[Byte] = tpe.serialize(t)
  def deserialize(bytes: Array[Byte]): Either[String, T] = tpe.deserialize(bytes)

  def values: Map[String, Any]

  def selectDynamic(str: String): Any = values(str)


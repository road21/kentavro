package kentavro

trait KSchema[T] extends Selectable:
  def avroType: AvroType[T]

  def serialize(t: T): Array[Byte]                       = avroType.serialize(t)
  def deserialize(bytes: Array[Byte]): Either[String, T] = avroType.deserialize(bytes)

  def values: Map[String, Any]

  def selectDynamic(str: String): Any = values(str)

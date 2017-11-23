package defaults

trait DefaultValue[T] {
  def default(): T
}

object DefaultValue {
  def apply[T](implicit myTypeClass: DefaultValue[T]): DefaultValue[T] = myTypeClass
}

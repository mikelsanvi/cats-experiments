package condenser

trait Condenser[T] {
  def condense(o1: T, o2: T): T
}

object Condenser {
  implicit def optCondenser[T](implicit tCondenser: Condenser[T]): Condenser[Option[T]] = {
    new Condenser[Option[T]] {
      override def condense(o1: Option[T], o2: Option[T]): Option[T] = {
        (o1, o2) match {
          case (Some(o1), Some(o2)) => Some(tCondenser.condense(o1, o2))
          case (o1, o2) => o1.orElse(o2)
        }
      }
    }
  }
}

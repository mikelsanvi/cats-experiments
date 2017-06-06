package functors

/**
  * Created by mikelsanvicente on 6/5/17.
  */
trait Printable[A] {
  def format(value: A): String
  def contramap[B](func: B => A): Printable[B] =
    new Printable[B] {
      override def format(value: B): String = Printable.this.format(func(value))
    }
}


object Printable {
  implicit val stringPrintable =
    new Printable[String] {
      def format(value: String): String =
        "\"" + value + "\""
    }
  implicit val booleanPrintable =
    new Printable[Boolean] {
      def format(value: Boolean): String =
        if(value) "yes" else "no"
    }

  implicit def boxPrintable[A](implicit aPrintable: Printable[A]): Printable[Box[A]] = {
    aPrintable.contramap(_.value)
  }
}

final case class Box[A](value: A)
/**
  * Created by mikelsanvicente on 5/9/17.
  */
trait Printable[A] {

  def format(a: A): String
}

object Printable {
  implicit val StringPrintable = new Printable[String] {
    override def format(a: String): String = a
  }

  implicit val IntPrintable = new Printable[Int] {
    override def format(a: Int): String = a.toString
  }
}

object PrintSyntax {

  implicit class PrintableOps[A](a: A)(implicit printable: Printable[A]) {
    def format(): String = printable.format(a)

    def print(): Unit = println(format())
  }

}

object MyTest extends App {

  import PrintSyntax._

  final case class Cat(
                        name: String,
                        age: Int,
                        color: String
                      )

  implicit val catPrintable = new Printable[Cat] {
    override def format(a: Cat): String = s"${a.name.format()} is a ${a.age.format()} year-old ${a.color.format()} cat."
  }

  val cat = Cat("felix", 4, "balck")

  cat.print()
}
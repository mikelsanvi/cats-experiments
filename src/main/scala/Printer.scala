/**
  * Created by mikelsanvicente on 6/15/17.
  */
trait Printer[T] {
  def print(v: T): String
}

object Printer {

  object implicits {
    implicit class TypeClassSyntaxInterface[T](v: T)(implicit printer: Printer[T]) {
      def print(): String = printer.print(v)
    }
  }

  implicit val stringPrinter = new Printer[String] {
    def print(v: String): String = v
  }

  implicit val longPrinter = new Printer[Long] {
    def print(v: Long): String = s"number($v)"
  }

  implicit def tPrinter[T](): Printer[T] = new Printer[T] {
    override def print(v: T): String = "Default printer:" + v.toString
  }

  implicit def tuplePrinter[T, V](implicit tPrinter: Printer[T], vPrinter: Printer[V]): Printer[(T,V)] = new Printer[(T, V)] {
    override def print(v: (T, V)): String = s"(${tPrinter.print(v._1)}, ${vPrinter.print(v._2)})"
  }

  def apply[T](implicit printer: Printer[T]): Printer[T] = printer

}


object Main extends App {

  import Printer.implicits._
//  val printer = Printer[Long]
//  //val floatPrinter = Printer[Float]
//  val tuplePrinter = Printer[(String, Long)]
//  //val printer = Printer[Float]
//
//  //println(floatPrinter.print(3f))
//  println(tuplePrinter.print(("hello", 4)))
  val tuple = ("hello", 4L)
  println(tuple.print())
}
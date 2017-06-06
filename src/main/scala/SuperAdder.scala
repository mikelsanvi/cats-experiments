/**
  * Created by mikelsanvicente on 5/25/17.
  */
import cats.Monoid

import cats.instances.int._
import cats.instances.double._
import cats.instances.string._
import cats.instances.set._

class SuperAdder {

  def add[A: Monoid](items: List[A])(): A = {
    items.fold(Monoid[A].empty)(Monoid[A].combine)
  }

  def addOptions(items: List[Option[Int]])(implicit intOptMonoid: Monoid[Option[Int]],intMonoid: Monoid[Int]): Int = {
    items.fold(intOptMonoid.empty)(intOptMonoid.combine).getOrElse(intMonoid.empty)
  }



}
case class Order(totalCost: Double, quantity: Double)


object SuperAdderTests extends App {

  implicit val orderMonoid = new Monoid[Order] {
    override def empty: Order = Order(0, 0)

    override def combine(x: Order, y: Order): Order = Order(
      x.totalCost + y.totalCost,
      x.quantity + y.quantity
    )
  }

  val adder = new SuperAdder()

  println(adder.add(List(1,2,3,4)))
  println(adder.add(List(1d,2.4,3.5,4.7)))
  println(adder.add(List("a","b","c","d","e")))
  println(adder.add(List(Order(3, 4), Order(1, 2))))
}

object Application extends App {
  import cats.syntax.monoid._
  import cats.syntax.option._
  import cats._

  val m = Monoid[Set[Int]]
  //  val setMonoid = Semigroup[Set[Int]]
  println(m.combine(Set(1), Set(4)) )

  println(Set(1) |+| Set(4) )

  val some = Some(1)
  val optionInt: Option[Int] = 1.some
}
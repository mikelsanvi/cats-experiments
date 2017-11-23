import java.time.LocalDate

import shapeless.HNil

/**
  * Created by mikelsanvicente on 6/8/17.
  */
object Test {

  def isBetweenDateOption(date: LocalDate, minimumDate: Option[LocalDate], maximumDate: Option[LocalDate]) = {
    minimumDate.forall(_.compareTo(date) <= 0) && maximumDate.forall(date.compareTo(_) <= 0)
  }


  case class Invoice(price: Double, quantity: Int)

  val invoice1: Invoice = ???
  val invoice2: Invoice = ???

  // Split Invoice data in small pieces (1 per column)
  // find operation for each data type
  // build back from splitted to Invoice

  val tuple1 = Invoice.unapply(invoice1)

  // HList => Heterogeneus lists

  val hlist = 1 :: "" :: HNil

  List(1,2,3,4)

  val res: Invoice = Invoice(invoice1.price + invoice2.price, invoice1.quantity + invoice2.quantity)

  val list = List[Int](10, 2, 3, 4)

  list.sorted

  val optList: List[Option[Int]] = List(Some(1), None, Some(4))
  optList.sorted

  val map = Map[String, Int]("key" -> 1, "key2" -> 1)

  val res1 = map.map(_._1)

  val res2 = map.map {
    case (k, v) =>
      k -> (v + 1)
  }

  trait A[A]
  case class B[C[_], T](v: C[T]) extends A[C[T]]

  val b: B[Option, Int] = B(Option(1))
  val a: A[Option[Int]] = B(Option(1))


  def process[A, C[_], T](b: C[T])(implicit typeEq: C[T] =:= A): A = {
    b
  }

  process2(1, "", 3.0d)(???)

  def process2[A, B, C](a: A, b:B, c:C)(f: (A,B) => C): (A, B, C) = ???

//  process[Int, .., ...]

  trait Ordering[T] {
    def compare(v1: T, v2:T): Int
  }

  object Ordering {
    implicit val intOrdering = new Ordering[Int] {
      override def compare(v1: Int, v2: Int): Int = v1.compareTo(v2)
    }

    // Option => Option[String]
    implicit def optionOrdering[T](implicit tOrdering: Ordering[T]): Ordering[Option[T]] = new Ordering[Option[T]] {
      override def compare(v1: Option[T], v2: Option[T]): Int = {
        (v1, v2) match {
          case (Some(t1), Some(t2)) => tOrdering.compare(t1, t2)
          case (None, _) => 1
          case (_, None) => -1
        }
      }
    }
  }


}

package monoid

import cats.kernel.Semigroup

/**
  * NonEmptyList addition
  *
  * Created by mikelsanvicente on 5/25/17.
  */
trait Semigroup[A] {
  def combine(x: A, y: A): A
}

object Semigroup {
  def apply[A](implicit semigroup: Semigroup[A]) =
    semigroup

  implicit def setIntersectionSemigroup[A]: Semigroup[Set[A]] = new Semigroup[Set[A]] {
    def combine(a: Set[A], b: Set[A]) =
      a intersect b
  }
}

// 1 + 2 + 3 == 1 + (2 + 3) == (1 + 2) + 3

// 0

// any Int + 0 = same Int

/**
  * LAWS
  *  Associativity
  *  Identity
  * int addition
  * int mult
  * String concat
  * List addition
  *
  * @tparam A
  */

trait Monoid[A] extends Semigroup[A] {
  def empty: A
}
  // Monoid[AA] <= Monoid[B]

object Monoid {
  def apply[A](implicit monoid: Monoid[A]) =
    monoid

  implicit val and = new Monoid[Boolean] {
    override def empty: Boolean = true

    override def combine(x: Boolean, y: Boolean): Boolean = x && y
  }

//  implicit val or = new Monoid[Boolean] {
//    override def empty: Boolean = false
//
//    override def combine(x: Boolean, y: Boolean): Boolean = x || y
//  }


  implicit def setAdd[A] = new Monoid[Set[A]] {
    override def empty: Set[A] = Set()

    override def combine(x: Set[A], y: Set[A]): Set[A] = x ++ y
  }

}


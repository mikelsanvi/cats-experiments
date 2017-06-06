package functors

import cats.Functor

/**
  * Created by mikelsanvicente on 6/5/17.
  */
sealed trait Tree[+A]
final case class Branch[A](left: Tree[A], right: Tree[A])
  extends Tree[A]
final case class Leaf[A](value: A) extends Tree[A]

object Tree {
  implicit val treeFunctor = new Functor[Tree] {
    override def map[A, B](fa: Tree[A])(f: (A) => B): Tree[B] = fa match {
      case Branch(left, right) => Branch(map(left)(f),map(right)(f))
      case Leaf(v) => Leaf(f(v))
    }
  }

  def branch[A](left: Tree[A], right: Tree[A]): Tree[A] = Branch(left, right)
  def leaf[A](value: A): Tree[A] =
    Leaf(value)
}
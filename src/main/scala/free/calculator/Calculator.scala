package free.calculator

import cats.free.Free._
import cats.free.Free
import cats._
import scala.language.higherKinds


/**
  * Created by mikelsanvicente on 4/27/17.
  */
class Calculator {

}

trait Operation[T]

case class Sum[T](v1: T, v2: T, op: (T, T) => T) extends Operation[T]

case class Substract[T](v1: T, v2: T, op: (T, T) => T) extends Operation[T]

case class Div[T](v1: T, v2: T, op: (T, T) => T) extends Operation[T]

object Operation {
  implicit def pure[T](v: T): Free[Operation, T] =
    Free.pure(v)

  implicit def operators[A](p: Free[Operation, A]) = new OperationOps[A](p)

  implicit def asIntParser[A](a: A)(implicit f: A => Free[Operation, Int]):
    OperationOps[Int] = new OperationOps(f(a))

  implicit def asDoubleParser[A](a: A)(implicit f: A => Free[Operation, Double]):
    OperationOps[Double] = new OperationOps(f(a))



  //implicit def asAParser[A](a: A): OperationOps[A] = new OperationOps(a)

  class OperationOps[T](calculatorProgram: Free[Operation, T]) {

    def |+|(v2: Free[Operation, T])(implicit op: Operations[T]): Free[Operation, T] = {
      for {
        v1 <- calculatorProgram
        v2 <- v2
        res <- liftF[Operation, T](Sum(v1, v2, op.sum))
      } yield (res)
    }

    def |-|(v2: Free[Operation, T])(implicit op: Operations[T]): Free[Operation, T] = {
      for {
        v1 <- calculatorProgram
        v2 <- v2
        res <- liftF[Operation, T](Substract(v1, v2, op.substract))
      } yield (res)
    }

    def |/|(v2: Free[Operation, T])(implicit op: Operations[T]): Free[Operation, T] = {
      for {
        v1 <- calculatorProgram
        v2 <- v2
        res <- liftF[Operation, T](Div(v1, v2, op.divide))
      } yield (res)
    }
  }

}

case class Operations[T](sum: (T, T) => T, substract: (T, T) => T, divide: (T, T) => T)

object Operations {
  implicit val IntOperations = Operations[Int](_ + _, _ - _, _ / _)
  implicit val DoubleOperations = Operations[Double](_ + _, _ - _, _ / _)
}


object Test extends App {
  import Operation._

  val doubleProg =  1.0 |-| 2.0 |/| 2.0
  val intProg = 1 |-| 2 |/| 2

  val program1 = doubleProg |-| intProg.map(_.toDouble)
  val program2 = doubleProg.map(_.toInt) |-| intProg

  val interp = interpreter

  println(program1.foldMap(interp))
  println(program2.foldMap(interp))

  def interpreter: Operation ~> Id =
    new (Operation ~> Id) {
      def apply[A](fa: Operation[A]): Id[A] = {
        fa match {
          case Sum(v1, v2, op) => op(v1, v2)
          case Substract(v1, v2, op) => op(v1, v2)
          case Div(v1, v2, op) => op(v1, v2)
        }
      }
    }



//  def printOps: Operation ~> Id =
//    new (Operation ~> Id) {
//      def apply[A](fa: Operation[A]): Id[String] = {
//        fa match {
//          case Sum(v1, v2, op) => "(" + v1.toString + " + " + v2.toString + ")"
//          case Substract(v1, v2, op) => "(" + v1.toString + " + " + v2.toString + ")"
//        }
//      }
//    }

}
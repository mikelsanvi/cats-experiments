package free

import cats.free.Free
import cats.free.Free.liftF
import free.calculator.{Operation, Operations, Substract, Sum}

/**
  * Created by mikelsanvicente on 4/28/17.
  */
package object int extends IntOperations

trait IntOperations {
  implicit def pure(v: Int): Free[Operation, Int] =
    Free.pure(v)

  implicit class OperationOps[T](calculatorProgram: Free[Operation, T]) {

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
  }
}
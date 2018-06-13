package io

import cats.data.EitherT

import scala.collection.mutable
import cats.effect._
import cats.instances.either._
import cats.syntax.all._

object IOTests extends App {
  type Error = String
  type IOOrError[A] = EitherT[IO, Error, A]

  val users = mutable.Map("mikel" -> "a", "silvia" -> "b")
  val balances = mutable.Map("mikel" -> 1000, "silvia" -> 1000)

  val loggedIn = mutable.Set.empty[String]

  val program = for {
    logged <- login("mikel", "a")
    _ <- log("mikel is logged in")
    message <- deposit("mikel", 100)
    _ <- log(message)
    balance <- getBalance("mikel")
    _ <- log(s"mikel has $balance")
    message2 <- deposit("karl", 1)
    _ <- log(message2)
  } yield ()

  log("should not appear")

  program.value.unsafeRunSync()

  private def login(user: String, psw: String): IOOrError[Boolean] = EitherT {
    IO {
      if (users.get(user).contains(psw)) {
        loggedIn += user
      }
      Either.cond(balances.contains(user), true, s"$user doesn't exist")
    }
  }

  private def log(message: String): IOOrError[Unit] =
    IO(println(message)).to[IOOrError]


  private def getBalance(user: String): IOOrError[Int] = EitherT[IO, String, Int] {
    IO {
      balances.get(user).toRight(s"$user does not have an account")
    }
  }

  private def isLoggedIn(user: String): IO[Boolean] =
    IO(loggedIn.contains(user))


  private def deposit(user: String, amount: Int): IOOrError[String] = {
    for {
      logged <- isLoggedIn(user).to[IOOrError]
      res <- EitherT.cond[IO](logged && amount > 0, {
        balances += ((user, balances.get(user).getOrElse(0) + amount))
        s"$user deposited $amount"
      }, if(logged) "Amount must be greater than 0" else s"$user is not logged")
    } yield res
  }
}

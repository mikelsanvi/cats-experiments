import cats.Show

/**
  * Created by mikelsanvicente on 5/9/17.
  */
object CatShow extends App{
  import cats.instances.int._
  import cats.instances.string._
  import cats.syntax.show._
  final case class Cat(
                        name: String,
                        age: Int,
                        color: String
                      )

  implicit val catShow = Show.show[Cat] {
    a => s"${a.name.show} is a ${a.age.show} year-old ${a.color.show} cat."
  }

  val cat = Cat("felix", 4, "balck")

  println(cat.show)
  cat.show

  import Syntax._

  cat.doSomehing()
}

object Syntax {
  implicit class Ops[A](a: A)(implicit showA: Show[A]) {
    def doSomehing():String = showA.show(a)
  }
}
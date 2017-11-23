/**
  * Created by mikelsanvicente on 5/9/17.
  */
object EqCat extends App {
  import cats.Eq
  import cats.syntax.eq._
  import cats.instances.all._
  final case class Cat(name: String, age: Int, color: String)

  implicit val eqCat = Eq.instance[Cat] {
    (cat1, cat2) =>
      cat1.name === cat2.name &&
        cat1.age === cat2.age &&
        cat1.color === cat2.color
  }
  object BankAccount {
    private var balance: Long = 0L

    def deposit(amount: Long): Unit =
      this.synchronized {
        balance += amount
      }
  }
  val cat1 = Cat("felix", 3, "black")
  val cat2 = Cat("garfield", 3, "orange")

  println(cat1 === cat2)
  println(cat1 === cat1)
  "sdf" === ""
}

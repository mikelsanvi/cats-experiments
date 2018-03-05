package smelter

import scala.reflect.ClassTag
import scala.util.Try

trait StringConverter[A] {
  def convert(s: String): Option[A]
  def typeName: String

  def toString(a: A): String = a.toString
}

object StringConverter {
  implicit val stringToInt: StringConverter[Int] = createConverter(_.toInt)
  implicit val stringToLong: StringConverter[Long] = createConverter(_.toLong)
  implicit val stringToString: StringConverter[String] = createConverter(identity)
  implicit val stringToBoolean: StringConverter[Boolean] = createConverter(_.toBoolean)
  implicit val stringToDouble: StringConverter[Double] = createConverter(_.toDouble)

  private def createConverter[A: ClassTag](f: String => A): StringConverter[A] = new StringConverter[A] {
    override def convert(s: String): Option[A] = Try(f(s)).toOption

    override def typeName: String = implicitly[ClassTag[A]].toString()
  }
}

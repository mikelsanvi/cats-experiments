package functors

import scala.util.Try

/**
  * Created by mikelsanvicente on 6/5/17.
  */
trait Codec[A] {
  self =>
  def encode(value: A): String
  def decode(value: String): Option[A]
  def imap[B](dec: A => B, enc: B => A): Codec[B] =
    new Codec[B] {
      override def encode(value: B): String = self.encode(enc(value))

      override def decode(value: String): Option[B] = self.decode(value).map(dec)
    }
}

object Codec {
  implicit val intCodec = new Codec[Int] {
    override def encode(value: Int): String = value.toString

    override def decode(value: String): Option[Int] = Try{value.toInt}.toOption
  }

  implicit def boxCodec[A](implicit aCodec: Codec[A]): Codec[Box[A]] =
    aCodec.imap(Box(_), _.value)
}

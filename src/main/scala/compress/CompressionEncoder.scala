package compress

import shapeless._
import shapeless.ops.hlist.IsHCons

/**
  * Created by mikelsanvicente on 4/28/17.
  */
trait CompressionEncoder[A] {
  type Compressed

  def compress(value: Seq[A]): Compressed

  def decompress(value: Compressed): Seq[A]
}
 // df.select($"name1", $"name2")

object CompressionEncoder {

  type EncoderAux[T, Compr] = CompressionEncoder[T] {type Compressed = Compr}

  def apply[A](implicit enc: CompressionEncoder[A]): EncoderAux[A, enc.Compressed] = enc

  def createEncoder[A, Repr](compressF: Seq[A] => Repr, decompressF: Repr => Seq[A]): EncoderAux[A, Repr] =
    new CompressionEncoder[A] {
      override type Compressed = Repr

      override def compress(value: Seq[A]): Repr = compressF(value)

      override def decompress(value: Repr): Seq[A] = decompressF(value)
    }

  private def compress[A](value: Seq[A]): CompressedBase[A] = CompressedBase(
    value.head,
    value.tail.foldLeft((value.head, List[Option[A]](None))) {
      case ((last, res), v) =>
        if (last == v)
          (last, None :: res)
        else
          (v, Some(v) :: res)
    }._2.reverse
  )

  private def decompress[A](value: CompressedBase[A]): Seq[A] =
    value.snapshots.tail.foldLeft(List(value.base)) {
      (res, elem) =>
        elem match {
          case Some(v) =>
            v :: res
          case None =>
            res.head :: res
        }
    }.reverse


  implicit val stringEncoder = createEncoder[String, CompressedBase[String]](compress, decompress)

  implicit val intEncoder = createEncoder[Int, CompressedBase[Int]](compress, decompress)

  implicit val longEncoder = createEncoder[Long, CompressedBase[Long]](compress, decompress)

  implicit val doubleEncoder = createEncoder[Double, CompressedBase[Double]](compress, decompress)

  implicit val floatEncoder = createEncoder[Float, CompressedBase[Float]](compress, decompress)

  implicit val booleanEncoder = createEncoder[Boolean, CompressedBase[Boolean]](compress, decompress)

  implicit val hnilEncoder: EncoderAux[HNil, HNil] = createEncoder(seq => HNil, _ => Seq(HNil))

  implicit def hlistEncoder[H, T <: HList, Compr1, Compr2 <: HList](implicit hEncoder: EncoderAux[H, Compr1],
                                                                    tEncoder: EncoderAux[T, Compr2]): EncoderAux[H :: T, ::[Compr1, Compr2]] =
    createEncoder(
      {
        seq =>
          val hs = seq.map(_.head)
          val ts = seq.map(_.tail)
          hEncoder.compress(hs) :: tEncoder.compress(ts)
      }, {
        compr =>
          val hs = hEncoder.decompress(compr.head)
          val ts = tEncoder.decompress(compr.tail)

          val tails = if (ts.head == HNil)
            List.fill(hs.size)(ts.head)
          else
            ts

          hs.zip(tails).map {
            case (h, t) => h :: t
          }
      }
    )

  implicit def genericEncoder[A, Repr <: HList, Head, Tail <: HList, Compr](
                                                                             implicit gen: Generic.Aux[A, Repr],
                                                                             enc: Lazy[EncoderAux[Repr, Compr]],
                                                                             isHCons: IsHCons.Aux[Repr, Head, Tail]
                                                                           ): EncoderAux[A, Compr] =
    createEncoder(
      {
        seq =>
          val hLists = seq.map(gen.to(_))
          enc.value.compress(hLists)
      }, {
        compr =>
          val hLists = enc.value.decompress(compr)
          hLists.map(gen.from(_))
      }
    )

  case class CompressedBase[A](base: A, snapshots: Seq[Option[A]]) {
    def decompress: Seq[A] = {
      snapshots.tail.foldLeft(List(base)) {
        (res, v) =>
          v.getOrElse(res.head) :: res
      }.reverse
    }
  }

}

object TestCompress extends App {
  val seq = (1 to 10000).map {
    i =>
      Type1("name", if (i % 3 == 0) Type2("test", i % 2) else Type2("test1", 1))
  }

  val enc = CompressionEncoder[Type1]

  val res = enc.compress(seq)

  println(enc.decompress(res) == seq)

  println(Typeable[enc.Compressed].describe)

}
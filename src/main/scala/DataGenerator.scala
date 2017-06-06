import shapeless._
import shapeless.ops.hlist.IsHCons

import scala.reflect.ClassTag
import scala.util.Random

/**
  * Created by mikelsanvicente on 4/28/17.
  */
trait DataGenerator[A] {

  def randomSeq(n: Int, rnd: Random = new Random()): Seq[A] = generate(rnd).take(n).toList

  def random(rnd: Random = new Random()): A = generate(rnd).head

  protected def generate(rnd: Random): Stream[A]

  def map[B](f: A => B): DataGenerator[B] = new DataGenerator[B] {
    override protected def generate(rnd: Random): Stream[B] =
      DataGenerator.this.generate(rnd).map(f)
  }
}

object DataGenerator {

  def apply[A](implicit gen: DataGenerator[A]): DataGenerator[A] = gen

  private def dataGenerator[A](f: Random => A): DataGenerator[A] =
    new DataGenerator[A] {
      override def generate(random: Random): Stream[A] = {
        f(random) #:: generate(random)
      }
    }

  implicit val stringGen = dataGenerator(rnd => rnd.alphanumeric.take(1 + rnd.nextInt(9)).mkString)

  implicit val intGen = dataGenerator(rnd => rnd.nextInt())

  implicit val longGen = dataGenerator(rnd => rnd.nextLong())

  implicit val shortGen = dataGenerator(rnd => rnd.nextInt(1 << 16).toShort)

  implicit val doubleGen = dataGenerator(rnd => rnd.nextDouble())

  implicit val floatGen = dataGenerator(rnd => rnd.nextFloat())

  implicit val booleanGen = dataGenerator(rnd => rnd.nextBoolean())

  implicit val byteGen = dataGenerator(rnd => (rnd.nextInt(256) - 128).toByte)

  implicit val charGen = dataGenerator(rnd => rnd.alphanumeric.head)

  implicit def optionGen[A](implicit genA: DataGenerator[A]) =
    dataGenerator {
      rnd =>
        if (rnd.nextBoolean())
          Some(genA.random(rnd))
        else
          None
    }

  implicit def seqGen[A](implicit genA: DataGenerator[A]) =
    dataGenerator[Seq[A]] {
      rnd =>
        val length = rnd.nextInt(10)
        genA.generate(rnd).take(length).toList
    }

  implicit def listGen[A](implicit genSeq: DataGenerator[Seq[A]]) = genSeq.map(_.toList)

  implicit def arrayGen[A: ClassTag](implicit genSeq: DataGenerator[Seq[A]]) = genSeq.map(_.toArray)

  implicit def setGen[A](implicit genSeq: DataGenerator[Seq[A]]) = genSeq.map(_.toSet)

  implicit def mapGen[K, V](implicit genK: DataGenerator[K], genV: DataGenerator[V]) =
    dataGenerator {
      rnd =>
        val length = rnd.nextInt(10)
        val seqK = genK.generate(rnd).take(length)
        val seqV = genV.generate(rnd).take(length)
        seqK.zip(seqV).toMap
    }

  implicit def mapImmutableGen[K, V](implicit genMap: DataGenerator[Map[K, V]]) =
    genMap.map[scala.collection.Map[K, V]](identity)

  implicit val hnilEncoder: DataGenerator[HNil] = dataGenerator(rnd => HNil)

  implicit def hlistEncoder[H, T <: HList](implicit hGen: DataGenerator[H],
                                           tGen: DataGenerator[T]): DataGenerator[H :: T] =
    dataGenerator(rnd => hGen.random(rnd) :: tGen.random(rnd))

  implicit def genericEncoder[A, Repr <: HList, Head, Tail <: HList](
                                                                      implicit gen: Generic.Aux[A, Repr],
                                                                      dataGen: Lazy[DataGenerator[Repr]],
                                                                      isHCons: IsHCons.Aux[Repr, Head, Tail]
                                                                    ): DataGenerator[A] =
    dataGenerator(rnd => gen.from(dataGen.value.random(rnd)))
}

object Test extends App {
  println(DataGenerator[MyData].randomSeq(10).mkString("\n"))
}

case class MyData(f: Int, seq: MyData2, opt: Option[String])

case class MyData2(f: Int, seq: Array[Short], opt: Option[String])
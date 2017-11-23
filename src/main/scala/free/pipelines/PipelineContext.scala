package free.pipelines

/**
  * Created by mikelsanvicente on 4/27/17.
  */

import cats.free.Free
import cats.free.Free._
import cats.{Id, ~>}
import free.pipelines.ExtractionDsl._
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe._

/**
  * Created by mikelsanvicente on 4/27/17.
  */
class PipelineContext[Container[_]](interpreter: Interpreter[Container]) {

  def extract[T <: Product](dataset: Container[T]): Extraction[Container, T] =
    Free.liftF(Source[Container, T](dataset))

  object syntax {

    implicit class ExtractionProgramOps[T](extractionProgram: Extraction[Container, T]) {

      def filter(f: T => Boolean): Extraction[Container, T] =
        for {
          extr <- extractionProgram
          res <- liftF(Filter(extr, f))
        } yield (res)

      def mapping[V <: Product : TypeTag](f: T => V): Extraction[Container, V] =
        for {
          extr <- extractionProgram
          res <- liftF(Mapping(extr, f))
        } yield (res)

      def load(): Seq[T] = interpreter.load(extractionProgram)

      def ++(other: Container[T]): Extraction[Container, T] =
        for {
          extr <- extractionProgram
          res <- liftF(Union(extr, other))
        } yield (res)

      def --(other: Container[T]): Extraction[Container, T] =
        for {
          extr <- extractionProgram
          res <- liftF(Substract(extr, other))
        } yield (res)
    }

    implicit class ContainerOps[T](container: Container[T]) {
      def filter(f: T => Boolean): Extraction[Container, T] = liftF(Filter(container, f))

      def mapping[V <: Product : TypeTag](f: T => V): Extraction[Container, V] = liftF(Mapping(container, f))

      def ++(other: Container[T]): Extraction[Container, T] = liftF(Union(container, other))

      def --(other: Container[T]): Extraction[Container, T] = liftF(Substract(container, other))
    }
  }
}

trait Interpreter[Container[_]] {
  def eval[T](program: Free[ExtractionDsl, Container[T]]): Container[T] =
    program.foldMap(
      new (ExtractionDsl ~> Id) {
        def apply[A](fa: ExtractionDsl[A]): Id[A] = {
          fa match {
            case source: Source[Container, _] =>
              processSource(source).asInstanceOf[A]
            case mapping: Mapping[Container, _, _] =>
              processMapping(mapping).asInstanceOf[A]
            case filter: Filter[Container, _] =>
              processFilter(filter).asInstanceOf[A]
            case union: Union[Container, _] =>
              processUnion(union).asInstanceOf[A]
            case subsctract: Substract[Container, _] =>
              processSubstract(subsctract).asInstanceOf[A]
          }
        }
      }
    )

  private[this] def processSource[T](s: Source[Container, T]): Container[T] =
    source(s.dataset)

  private[this] def processMapping[T, V <: Product](mapping: Mapping[Container, T, V]): Container[V] = {
    implicit val vTypeTag = mapping.vTypeTag
    map(mapping.extraction, mapping.f)
  }

  private[this] def processFilter[T](f: Filter[Container, T]): Container[T] =
    filter(f.extraction, f.predicate)

  private[this] def processUnion[T](u: Union[Container, T]): Container[T] =
    union(u.extraction1, u.extraction2)

  private[this] def processSubstract[T](s: Substract[Container, T]): Container[T] =
    substract(s.extraction1, s.extraction2)

  def load[T](program: Free[ExtractionDsl, Container[T]]): Seq[T] = load(eval(program))

  def load[T](container: Container[T]): Seq[T]

  def source[T](container: Container[T]): Container[T]

  def map[T, V <: Product : TypeTag](container: Container[T], f: T => V): Container[V]

  def filter[T](container: Container[T], p: T => Boolean): Container[T]

  def union[T](container1: Container[T], container2: Container[T]): Container[T]

  def substract[T](container1: Container[T], container2: Container[T]): Container[T]
}

sealed abstract class ExtractionDsl[ContainerT]

private object ExtractionDsl {

  case class Source[Container[_], T](dataset: Container[T]) extends ExtractionDsl[Container[T]]

  case class Mapping[Container[_], T, V <: Product : TypeTag](extraction: Container[T], f: T => V)
    extends ExtractionDsl[Container[V]] {
    def vTypeTag: TypeTag[V] = typeTag[V]
  }

  case class Filter[Container[_], T](extraction: Container[T], predicate: T => Boolean) extends ExtractionDsl[Container[T]]

  case class Union[Container[_], T](extraction1: Container[T], extraction2: Container[T]) extends ExtractionDsl[Container[T]]

  case class Substract[Container[_], T](extraction1: Container[T], extraction2: Container[T]) extends ExtractionDsl[Container[T]]

}

class SeqInterpreter extends Interpreter[Seq] {

  override def load[T](container: Seq[T]): Seq[T] = container

  override def source[T](container: Seq[T]): Seq[T] = container

  override def map[T, V <: Product : TypeTag](container: Seq[T], f: T => V): Seq[V] = container.map(f)

  override def filter[T](container: Seq[T], p: T => Boolean): Seq[T] = container.filter(p)

  override def union[T](container1: Seq[T], container2: Seq[T]): Seq[T] = container1 ++ container2

  override def substract[T](container1: Seq[T], container2: Seq[T]): Seq[T] = container1 diff container2
}

class SparkInterpreter(sparkSession: SparkSession) extends Interpreter[Dataset] {

  private[this] lazy val sqlContext = sparkSession.sqlContext
  import sqlContext.implicits._

  override def load[T](container: Dataset[T]): Seq[T] = container.collect()

  override def source[T](container: Dataset[T]): Dataset[T] = container

  override def map[T, V <: Product : TypeTag](container: Dataset[T], f: T => V): Dataset[V] = container.map(f)

  override def filter[T](container: Dataset[T], p: T => Boolean): Dataset[T] = container.filter(p)

  override def union[T](container1: Dataset[T], container2: Dataset[T]): Dataset[T] = container1 union container2

  override def substract[T](container1: Dataset[T], container2: Dataset[T]): Dataset[T] = container1 except container2
}
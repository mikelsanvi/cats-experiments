package free.pipelines

/**
  * Created by mikelsanvicente on 4/27/17.
  */

import cats.free.Free
import cats.free.Free._
import cats.{Id, ~>}
import free.pipelines.ExtractionDsl._

/**
  * Created by mikelsanvicente on 4/27/17.
  */
abstract class PipelineContext[Container[_]] {

  def interpreter: Interpreter[Container]

  def extract[T <: Product](dataset: Container[T]): Free[ExtractionDsl, Container[T]] =
    Free.liftF(Source[Container, T](dataset))

  object PipelineDsl {
    implicit class ExtractionProgramOps[T](extractionProgram: Free[ExtractionDsl, Container[T]]) {
      def filter(f: T => Boolean): Free[ExtractionDsl, Container[T]] =
        for {
          extr <- extractionProgram
          res <- liftF(Filter(extr, f))
        } yield (res)

      def mapping[V](f: T => V): Free[ExtractionDsl, Container[V]] =
        for {
          extr <- extractionProgram
          res <- liftF(Mapping(extr, f))
        } yield (res)

      def load(): Container[T] = interpreter.eval[T](extractionProgram)
    }

    implicit class ContainerOps[A](container: Container[A]) {
      def ++(other: Container[A]): Free[ExtractionDsl, Container[A]] = liftF(Union(container, other))

      def --(other: Container[A]): Free[ExtractionDsl, Container[A]] = liftF(Substract(container, other))
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

  private[this] def processMapping[T, V](mapping: Mapping[Container, T, V]): Container[V] =
    map(mapping.extraction, mapping.f)

  private[this] def processFilter[T](f: Filter[Container, T]): Container[T] =
    filter(f.extraction, f.predicate)

  private[this] def processUnion[T](u: Union[Container, T]): Container[T] =
    union(u.extraction1, u.extraction2)

  private[this] def processSubstract[T](s: Substract[Container, T]): Container[T] =
    substract(s.extraction1, s.extraction2)


  def source[T](container: Container[T]): Container[T]

  def map[T, V](container: Container[T], f: T => V): Container[V]

  def filter[T](container: Container[T], p: T => Boolean): Container[T]

  def union[T](container1: Container[T], container2: Container[T]): Container[T]

  def substract[T](container1: Container[T], container2: Container[T]): Container[T]
}

sealed abstract class ExtractionDsl[ContainerT]

private object ExtractionDsl {

  case class Source[Container[_], T](dataset: Container[T]) extends ExtractionDsl[Container[T]]

  case class Mapping[Container[_], T, V](extraction: Container[T], f: T => V) extends ExtractionDsl[Container[V]]

  case class Filter[Container[_], T](extraction: Container[T], predicate: T => Boolean) extends ExtractionDsl[Container[T]]

  case class Union[Container[_], T](extraction1: Container[T], extraction2: Container[T]) extends ExtractionDsl[Container[T]]

  case class Substract[Container[_], T](extraction1: Container[T], extraction2: Container[T]) extends ExtractionDsl[Container[T]]

}

class SeqPipelineContext extends PipelineContext[Seq] {
  val interpreter: Interpreter[Seq] = new Interpreter[Seq] {
    override def source[T](container: Seq[T]): Seq[T] = container

    override def map[T, V](container: Seq[T], f: T => V): Seq[V] = container.map(f)

    override def filter[T](container: Seq[T], p: T => Boolean): Seq[T] = container.filter(p)

    def union[T](container1: Seq[T], container2: Seq[T]): Seq[T] = container1 ++ container2

    def substract[T](container1: Seq[T], container2: Seq[T]): Seq[T] = container1 diff container2
  }
}

object Test extends App {
  def run[Container[_]](pipelineContext: PipelineContext[Container], source: Container[Person]): Unit = {
    import pipelineContext.PipelineDsl._

    val program = for {
      mikel <- pipelineContext.extract(source).mapping(_.name).filter(_ == "mikel").mapping(_.toUpperCase())
      javier <- pipelineContext.extract(source).mapping(_.name).filter(_ == "javier")
      res <- mikel ++ javier
    } yield (res)

    println(program.load())
  }

  run(new SeqPipelineContext(), List(Person("mikel", "san vicente"), Person("javier", "san vicente")))


}

case class Person(name: String, surname: String)
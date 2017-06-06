package free.pipelines

/**
  * Created by mikelsanvicente on 4/27/17.
  */

import cats.free.Free
import cats.free.Free._
import cats.{Functor, Id, ~>}
import free.pipelines.ExtractionDsl.{Filter, Mapping, Source}

/**
  * Created by mikelsanvicente on 4/27/17.
  */
abstract class PipelineContext[C[_]] {

//  type My[Container[_]] = ExtractionDsl[Container, _]

  def extract[T <: Product](dataset: C[T]): Free[ExtractionDsl, Container[C, T]] = {
    val source = Source[C, T](dataset)
    Free.liftF(source)
  }

  def eval[T](program: Free[ExtractionDsl, Container[C, T]]): Container[C, T] =  {
    program.foldMap(interpreter)
  }

  def interpreter: ExtractionDsl ~> Id =
    new (ExtractionDsl ~> Id) {
      def apply[A](fa: ExtractionDsl[A]): Id[A] = {
        fa match {
          case source: Source[C, _] =>
            processSource(source).asInstanceOf[A]
          case mapping: Mapping[C, _, _] =>
            processMapping(mapping).asInstanceOf[A]
          case filter: Filter[C, _] =>
            processFilter(filter).asInstanceOf[A]
        }
      }
    }

  private[this] def processSource[T](s: Source[C, T]): Container[C, T] =
    lift(source(s.dataset))

  private[this] def processMapping[T, V](mapping: Mapping[C, T, V]): Container[C, V] ={
    lift(map(mapping.extraction, mapping.f))
  }
  private[this] def processFilter[T](f: Filter[C, T]): Container[C, T] = {
    lift(filter(f.extraction, f.predicate))
  }

  def lift[T](c: C[T]): Container[C, T]

  def source[T](container: C[T]): C[T]
  def map[T, V](container: C[T], f: T => V): C[V]
  def filter[T](container: C[T], p: T => Boolean): C[T]

  object PipelineDsl {

    implicit class ExtractionProgramOps[C[_], T](extractionProgram: Free[ExtractionDsl, Container[C, T]]) {

      def filter(f: T => Boolean): Free[ExtractionDsl, Container[C, T]] =
        for {
          extr <- extractionProgram
          res <- liftF(Filter(extr.container, f))
        } yield (res)

      def mapping[V](f: T => V): Free[ExtractionDsl, Container[C, V]] =
        for {
          extr <- extractionProgram
          res <- liftF(Mapping(extr.container, f))
        } yield (res)
    }

  }
}

trait Container[C[_], A] {
  def container: C[A]
  def ++(other: Container[C, A]): Container[C, A]
  def --(other: Container[C, A]): Container[C, A]
}

sealed abstract class ExtractionDsl[ContainerT]

private object ExtractionDsl {
  case class Source[C[_], T](dataset: C[T]) extends ExtractionDsl[Container[C, T]]

  case class Mapping[C[_], T, V](extraction: C[T], f: T => V) extends ExtractionDsl[Container[C, V]]

  case class Filter[C[_], T](extraction: C[T], predicate: T => Boolean) extends ExtractionDsl[Container[C, T]]
}

class SeqPipelineContext extends PipelineContext[Seq] {

  case class SeqContainer[A](container: Seq[A]) extends Container[Seq, A] {
    override def ++(other: Container[Seq, A]): Container[Seq, A] = this.copy(
      container = container ++ other.container
    )

    override def --(other: Container[Seq, A]): Container[Seq, A] = this.copy(
      container = container diff other.container
    )
  }

  override def lift[T](c: Seq[T]): Container[Seq, T] = SeqContainer(c)
  override def source[T](container: Seq[T]): Seq[T] = container
  override def map[T, V](container: Seq[T], f: T => V): Seq[V] = container.map(f)
  override def filter[T](container: Seq[T], p: T => Boolean): Seq[T] = container.filter(p)

}

object Test extends App {

  def run[Container[_]](pipelineContext: PipelineContext[Container], source: Container[Person]): Unit = {
    import pipelineContext.PipelineDsl._

    val program = for{
      mikel <- pipelineContext.extract(source).mapping(_.name).filter(_ == "mikel")
      javier <- pipelineContext.extract(source).mapping(_.name).filter(_ == "javier")
    } yield (mikel ++ javier)

    println(pipelineContext.eval(program).container)
  }

  run(new SeqPipelineContext(), List(Person("mikel", "san vicente"), Person("javier", "san vicente")))


}

case class Person(name: String, surname: String)
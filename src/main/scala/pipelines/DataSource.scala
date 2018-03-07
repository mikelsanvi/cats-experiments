package pipelines

import java.time.LocalDate

import org.apache.spark.sql.{Dataset, Encoder}

/** A source of data
  *
  * @tparam A The record type of the source
  */
sealed trait DataSource[A] { self =>

  def listSourceDates()(implicit pipelineContext: PipelineContext): Seq[LocalDate]

  def read(date: LocalDate)(implicit pipelineContext: PipelineContext): Option[Dataset[A]]

  def readLatest()(implicit pipelineContext: PipelineContext): Option[Dataset[A]]

  def read(from: Option[LocalDate], to: Option[LocalDate])(implicit pipelineContext: PipelineContext): Option[Dataset[A]]

  def map[B: Encoder](f: A => B): DataSource[B] = new DataSource[B] {

    override def listSourceDates()(implicit pipelineContext: PipelineContext): Seq[LocalDate] = self.listSourceDates()

    override def read(date: LocalDate)(implicit pipelineContext: PipelineContext): Option[Dataset[B]] =
      self.read(date).map(_.map(f))

    override def readLatest()(implicit pipelineContext: PipelineContext): Option[Dataset[B]] =
      self.readLatest().map(_.map(f))

    override def read(from: Option[LocalDate], to: Option[LocalDate])(implicit pipelineContext: PipelineContext): Option[Dataset[B]] =
      self.read(from, to).map(_.map(f))
  }
}

object DataSource {

  def apply[A: Encoder](dataSourceDescriptor: DatedSourceDescriptor[A]): DataSource[A] = new DataSource[A] {

    override def listSourceDates()(implicit pipelineContext: PipelineContext): Seq[LocalDate] =
      pipelineContext.listSourceDates(dataSourceDescriptor)

    override def read(date: LocalDate)(implicit pipelineContext: PipelineContext): Option[Dataset[A]] = {
      pipelineContext.readDataset(dataSourceDescriptor, date)
    }

    override def readLatest()(implicit pipelineContext: PipelineContext): Option[Dataset[A]] =
      pipelineContext.readLatestDataset(dataSourceDescriptor)

    override def read(from: Option[LocalDate], to: Option[LocalDate])(implicit pipelineContext: PipelineContext): Option[Dataset[A]] =
      pipelineContext.readDataset(dataSourceDescriptor, from, to)

  }

}

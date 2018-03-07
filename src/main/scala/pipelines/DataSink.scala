package pipelines

import java.time.LocalDate

import org.apache.spark.sql.{Dataset, Encoder, SaveMode}

/** Pipelines can write data to sinks
  *
  * @tparam A The record type
  */
sealed trait DataSink[A] { self =>

  def name: String

  def save(dataset: Dataset[A], date: LocalDate, saveMode: SaveMode = SaveMode.ErrorIfExists)(
      implicit pipelineContext: PipelineContext): Unit

  def contramap[B: Encoder](f: B => A)(implicit encoderA: Encoder[A]): DataSink[B] = new DataSink[B] {

    override def name: String = s"contramap-${self.name}"

    override def save(dataset: Dataset[B], date: LocalDate, saveMode: SaveMode)(implicit pipelineContext: PipelineContext): Unit =
      self.save(dataset.map(f), date, saveMode)

  }

}

object DataSink {

  def apply[A: Encoder](datasetDescriptor: DatedSourceDescriptor[A]): DataSink[A] = new DataSink[A] {

    override def name: String = datasetDescriptor.name

    override def save(dataset: Dataset[A], date: LocalDate, saveMode: SaveMode)(implicit pipelineContext: PipelineContext): Unit = {
      import pipelineContext.implicits._
      dataset.save(datasetDescriptor, date, saveMode)
    }

  }

}

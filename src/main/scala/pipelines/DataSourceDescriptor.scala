
package pipelines

import java.time.format.DateTimeFormatter

sealed trait DataSourceDescriptor[A] {
  def name: String

  private[pipelines] def format: Format

  def sourceOrigin: SourceOrigin

  def path: String
}

case class DatedSourceDescriptor[A](
    name: String,
    format: Format,
    formatter: DateTimeFormatter,
    sourceOrigin: SourceOrigin,
    path: String,
    suffix: Option[String]
) extends DataSourceDescriptor[A]

case class SingleFolderDataSourceDescriptor[A](
    name: String,
    format: Format,
    sourceOrigin: SourceOrigin,
    path: String
) extends DataSourceDescriptor[A]

object DataSourceDescriptor {

  /** Creates a dated data source descriptor
    *
    * @param name Name that identifies uniquely this source. This name will be used to load the source configurations
    * @param format The format of the source (JSON, Parquet, Text)
    * @param sourceOrigin The source origin (Pro, Siphon, Graph...)
    * @param path The relative path to the datasource
    * @param dateFormatter The formatter of the dates in the URL of this source
    * @tparam A The type of the data
    * @return
    */
  def dated[A](
      name: String,
      format: Format,
      sourceOrigin: SourceOrigin,
      path: String,
      suffix: Option[String] = None,
      dateFormatter: DateTimeFormatter = DateTimeFormatter.ISO_LOCAL_DATE
  ): DatedSourceDescriptor[A] =
    DatedSourceDescriptor(
      name = name,
      format = format,
      formatter = dateFormatter,
      sourceOrigin = sourceOrigin,
      path = path,
      suffix = suffix
    )

  /** Creates a single folder data source descriptor
    *
    * @param name Name that identifies uniquely this source. This name will be used to load the source configurations
    * @param format The format of the source (JSON, Parquet, Text)
    * @param sourceOrigin The source origin (Pro, Siphon, Graph...)
    * @param path The relative path to the datasource
    * @tparam A The type of the data
    * @return
    */
  def single[A](name: String, format: Format, sourceOrigin: SourceOrigin, path: String): SingleFolderDataSourceDescriptor[A] =
    SingleFolderDataSourceDescriptor(
      name = name,
      format = format,
      sourceOrigin = sourceOrigin,
      path = path
    )
}

case class SourceOrigin(name: String)

object SourceOrigin {
  val siphon = SourceOrigin("siphon")
  val profiling = SourceOrigin("profiling")
  val floodgate = SourceOrigin("floodgate")
  val reclamation = SourceOrigin("reclamation")
  val extractions = SourceOrigin("extractions")
  val smelter = SourceOrigin("smelter")
  val alchemy = SourceOrigin("alchemy")

  val sourceTypes: Seq[SourceOrigin] = Seq(siphon, profiling, floodgate, reclamation, extractions, smelter, alchemy)
}

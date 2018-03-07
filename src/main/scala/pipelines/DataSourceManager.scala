
package pipelines

import java.time.LocalDate

import pipelines.config.DatasourceConfig
import pipelines.utils.{DatedFolder, HadoopUtils}
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StructField, StructType}
import org.slf4j.LoggerFactory

/** The data source manager handles the physical location of the data.
  *
  */
trait DataSourceManager {

  /**
    * Retrieves a list with all the dates for the given dataset
    *
    * @param descriptor The source descriptor
    * @tparam A The data type
    * @return
    */
  def listDatasetDates[A](descriptor: DatedSourceDescriptor[A]): Seq[LocalDate]

  /**
    * Reads the data stored in The source descriptor for the given date
    *
    * @param descriptor The source descriptor
    * @tparam A The data type
    * @return
    */
  def read[A: Encoder](descriptor: SingleFolderDataSourceDescriptor[A]): DataFrame

  /**
    * Reads the data stored in The source descriptor for the given date. It will return None when the folder doesn't exist
    *
    * @param descriptor The source descriptor to read
    * @param date The date
    * @tparam A The data type
    */
  def read[A: Encoder](descriptor: DatedSourceDescriptor[A], date: LocalDate): Option[DataFrame]

  /**
    * Reads the data stored in The source descriptor for the given dates. It will return None when any of the dates exist
    *
    * @param descriptor The source descriptor
    * @param dates The list of dates
    * @tparam A the data type
    * @return
    */
  def read[A: Encoder](descriptor: DatedSourceDescriptor[A], dates: Seq[LocalDate]): Option[DataFrame]

  /**
    * Writes a dataset
    *
    * @param dataset The source descriptor to write
    * @param descriptor The source descriptor info
    * @param date The date
    * @param saveMode The spark save mode
    * @tparam A The data type
    */
  def write[A: Encoder](dataset: DataFrame, descriptor: DatedSourceDescriptor[A], date: LocalDate, saveMode: SaveMode): Unit

  /**
    * Writes a dataset
    *
    * @param dataset The source descriptor to write
    * @param descriptor The source descriptor info
    * @param saveMode The spark save mode
    * @tparam A The data type
    */
  def write[A: Encoder](dataset: DataFrame, descriptor: SingleFolderDataSourceDescriptor[A], saveMode: SaveMode): Unit

}

object DataSourceManager {

  def apply(config: DatasourceConfig, spark: SparkSession): DataSourceManager = {
    new DefaultDatasetManager(config, spark)
  }

  /**
    * The DefaultDatasetManager reads the data from the Hadoop filesystem
    *
    * @param config The pipeline configuration
    * @param spark  The spark session
    */
  class DefaultDatasetManager(config: DatasourceConfig, spark: SparkSession) extends DataSourceManager with Serializable {

    private val logger = LoggerFactory.getLogger(getClass)

    private val hadoopUtils = new HadoopUtils()

    override def read[A: Encoder](dataset: SingleFolderDataSourceDescriptor[A]): DataFrame = {
      read(Seq(config.path(dataset)), dataset)
    }

    override def read[A: Encoder](dataset: DatedSourceDescriptor[A], date: LocalDate): Option[DataFrame] = {
      val folder = buildFolder(dataset, date)
      if (hadoopUtils.exists(folder)) {
        Some(read(Seq(folder), dataset))
      } else {
        None
      }
    }
    override def read[A: Encoder](dataset: DatedSourceDescriptor[A], dates: Seq[LocalDate]): Option[DataFrame] = {
      if (dates.isEmpty) {
        None
      } else {
        val datesSet = dates.toSet
        val folders = listDatedFolders(dataset).filter(datedFolder => datesSet.contains(datedFolder.date)).map(_.path)
        if (folders.nonEmpty) {
          Some(read(folders, dataset))
        } else {
          None
        }
      }
    }

    override def write[A: Encoder](dataset: DataFrame, descriptor: DatedSourceDescriptor[A], date: LocalDate, saveMode: SaveMode): Unit = {
      val folder = buildFolder(descriptor, date)
      descriptor.format match {
        case JSON(compression) =>
          dataset.write
            .mode(saveMode)
            .option("compression", compression.name)
            .json(folder)
        case Parquet(compression) =>
          dataset.write
            .mode(saveMode)
            .option("compression", compression.name)
            .parquet(folder)
      }
    }

    override def write[A: Encoder](dataset: DataFrame, descriptor: SingleFolderDataSourceDescriptor[A], saveMode: SaveMode): Unit = {
      val folder = config.path(descriptor)
      descriptor.format match {
        case JSON(compression) =>
          dataset.write
            .mode(saveMode)
            .option("compression", compression.name)
            .json(folder)
        case Parquet(compression) =>
          dataset.write
            .mode(saveMode)
            .option("compression", compression.name)
            .parquet(folder)
      }
    }

    override def listDatasetDates[A](descriptor: DatedSourceDescriptor[A]): Seq[LocalDate] = {
      listDatedFolders(descriptor)
        .map { datedFolder =>
          datedFolder.date
        }
    }

    private def listDatedFolders[A](descriptor: DatedSourceDescriptor[A]): Seq[DatedFolder] = {
      val path = config.path(descriptor)
      logger.info(s"Listing folders for datasource ${descriptor.name} in $path")
      hadoopUtils
        .listDateFoldersWithSuccess(path, descriptor.formatter, descriptor.suffix)

    }

    protected[this] def read[A: Encoder](folder: Seq[String], descriptor: DataSourceDescriptor[A]): DataFrame = {
      descriptor.format match {
        case Parquet(_) =>
          spark.read.parquet(folder: _*)
        case JSON(_) =>
          val schema = implicitly[Encoder[A]].schema
          spark.read.schema(schema).json(folder: _*)
      }
    }

    private[this] def buildFolder[A](dataset: DatedSourceDescriptor[A], date: LocalDate): String = {
      // note: this can introduce some unnecessary `/`; Hadoop Path will remove them
      s"${config.path(dataset)}/${dataset.formatter.format(date)}/${dataset.suffix.getOrElse("")}"
    }

  }

}

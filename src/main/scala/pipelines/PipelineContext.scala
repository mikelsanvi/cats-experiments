
package pipelines

import java.time.LocalDate

import pipelines.config.DatasourceConfig
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

/** The pipeline context allows to read and write spark datasets / dataframes / rdds in a safe way.
  *
  * @param spark             The spark session
  * @param datasourceManager The data source manager that handle the physical location of the data
  * @param config            The pipeline configuration
  */
class PipelineContext(
    val spark: SparkSession,
    @transient val datasourceManager: DataSourceManager,
    val config: DatasourceConfig
) extends Serializable {

  implicit private val localDateOrdering: Ordering[LocalDate] = Ordering.by(_.toEpochDay)

  /**
    * Retrieves an ordered list of the dates where data is available for the given source
    *
    * @param descriptor The source descriptor
    * @tparam A The data type
    * @return
    */
  def listSourceDates[A](descriptor: DatedSourceDescriptor[A]): Seq[LocalDate] = {
    datasourceManager.listDatasetDates(descriptor).filter(date => config.horizon.forall(horizon => !date.isAfter(horizon))).sorted
  }

  /**
    * Reads the data source.
    *
    * @param descriptor The source descriptor
    * @tparam A The data type
    * @return
    */
  def readDataset[A: Encoder](descriptor: SingleFolderDataSourceDescriptor[A]): Dataset[A] = {
    readDataFrame(descriptor).as[A]
  }

  /**
    * Reads the dataset.
    *
    * @param descriptor The source descriptor
    * @param condition  The filter to apply
    * @tparam A The data type
    * @return
    */
  def readDataset[A: Encoder](descriptor: SingleFolderDataSourceDescriptor[A], condition: Column): Dataset[A] = {
    readDataFrame(descriptor, condition).as[A]
  }

  /**
    * Reads the latest folder for a dated dataset. It will return None when no folder is found
    *
    * @param descriptor The source descriptor
    * @tparam A The data type
    * @return
    */
  def readLatestDataset[A: Encoder](descriptor: DatedSourceDescriptor[A]): Option[Dataset[A]] = {
    readLatestDataFrame(descriptor).map(_.as[A])
  }

  /**
    * Reads the latest folder for a dated dataset. It will return None when no folder is found
    *
    * @param descriptor The source descriptor
    * @param condition  The filter to apply
    * @tparam A The data type
    * @return
    */
  def readLatestDataset[A: Encoder](descriptor: DatedSourceDescriptor[A], condition: Column): Option[Dataset[A]] = {
    readLatestDataFrame(descriptor).map(_.where(condition).as[A])
  }

  /**
    * Reads the data stored in the dataset for the given date. It will return None when the folder doesn't exist
    *
    * @param descriptor The source descriptor
    * @param date       The date
    * @tparam A The data type
    * @return
    */
  def readDataset[A: Encoder](descriptor: DatedSourceDescriptor[A], date: LocalDate): Option[Dataset[A]] = {
    readDataFrame(descriptor, date).map(_.as[A])
  }

  /**
    * Reads the data stored in the dataset for the given date. It will return None when the folder doesn't exist
    *
    * @param descriptor The source descriptor
    * @param date       The date
    * @param condition  The filter to apply
    * @tparam A The data type
    * @return
    */
  def readDataset[A: Encoder](descriptor: DatedSourceDescriptor[A], date: LocalDate, condition: Column): Option[Dataset[A]] = {
    readDataFrame(descriptor, date).map(_.where(condition).as[A])
  }

  /**
    * Reads the data stored in those folders between the given dates. It will return None when no folders are found
    *
    * @param descriptor The source descriptor
    * @param from       The min date (included)
    * @param to         The max date (excluded)
    * @tparam A The data type
    * @return
    */
  def readDataset[A: Encoder](descriptor: DatedSourceDescriptor[A], from: Option[LocalDate], to: Option[LocalDate]): Option[Dataset[A]] = {
    readDataFrame(descriptor, from = from, to = to).map(_.as[A])
  }

  /**
    * Reads the data stored in those folders between the given dates. It will return None when no folders are found
    *
    * @param descriptor The source descriptor
    * @param from       The min date (included)
    * @param to         The max date (excluded)
    * @param condition  The filter to apply
    * @tparam A The data type
    * @return
    */
  def readDataset[A: Encoder](
      descriptor: DatedSourceDescriptor[A],
      from: Option[LocalDate],
      to: Option[LocalDate],
      condition: Column
  ): Option[Dataset[A]] = {
    readDataFrame(descriptor, from = from, to = to).map(_.where(condition).as[A])
  }

  /**
    * Reads the dataset.
    *
    * @param descriptor The source descriptor
    * @tparam A The data type
    * @return
    */
  def readRDD[A: Encoder](descriptor: SingleFolderDataSourceDescriptor[A]): RDD[A] = {
    readDataFrame(descriptor).as[A].rdd
  }

  /**
    * Reads the dataset.
    *
    * @param descriptor The source descriptor
    * @param condition  The filter to apply
    * @tparam A The data type
    * @return
    */
  def readRDD[A: Encoder](descriptor: SingleFolderDataSourceDescriptor[A], condition: Column): RDD[A] = {
    readDataFrame(descriptor, condition).as[A].rdd
  }

  /**
    * Reads the latest folder for a dated dataset. It will return None when no folder is found
    *
    * @param descriptor The source descriptor
    * @tparam A The data type
    * @return
    */
  def readLatestRDD[A: Encoder](descriptor: DatedSourceDescriptor[A]): Option[RDD[A]] = {
    readLatestDataFrame(descriptor).map(_.as[A].rdd)
  }

  /**
    * Reads the latest folder for a dated dataset. It will return None when no folder is found
    *
    * @param descriptor The source descriptor
    * @param condition  The filter to apply
    * @tparam A The data type
    * @return
    */
  def readLatestRDD[A: Encoder](descriptor: DatedSourceDescriptor[A], condition: Column): Option[RDD[A]] = {
    readLatestDataFrame(descriptor).map(_.where(condition).as[A].rdd)
  }

  /**
    * Reads the data stored in the dataset for the given date. It will return None when the folder doesn't exist
    *
    * @param descriptor The source descriptor
    * @param date       The date
    * @tparam A The data type
    * @return
    */
  def readRDD[A: Encoder](descriptor: DatedSourceDescriptor[A], date: LocalDate): Option[RDD[A]] = {
    readDataFrame(descriptor, date).map(_.as[A].rdd)
  }

  /**
    * Reads the data stored in the dataset for the given date. It will return None when the folder doesn't exist
    *
    * @param descriptor The source descriptor
    * @param date       The date
    * @param condition  The filter to apply
    * @tparam A The data type
    * @return
    */
  def readRDD[A: Encoder](descriptor: DatedSourceDescriptor[A], date: LocalDate, condition: Column): Option[RDD[A]] = {
    readDataFrame(descriptor, date).map(_.where(condition).as[A].rdd)
  }

  /**
    * Reads the data stored in those folders between the given dates. It will return None when no folders are found
    *
    * @param descriptor The source descriptor
    * @param from       The min date (included)
    * @param to         The max date (excluded)
    * @tparam A The data type
    * @return
    */
  def readRDD[A: Encoder](descriptor: DatedSourceDescriptor[A], from: Option[LocalDate], to: Option[LocalDate]): Option[RDD[A]] = {
    readDataFrame(descriptor, from = from, to = to).map(_.as[A].rdd)
  }

  /**
    * Reads the data stored in those folders between the given dates. It will return None when no folders are found
    *
    * @param descriptor The source descriptor
    * @param from       The min date (included)
    * @param to         The max date (excluded)
    * @param condition  The filter to apply
    * @tparam A The data type
    * @return
    */
  def readRDD[A: Encoder](
      descriptor: DatedSourceDescriptor[A],
      from: Option[LocalDate],
      to: Option[LocalDate],
      condition: Column
  ): Option[RDD[A]] = {
    readDataFrame(descriptor, from = from, to = to).map(_.where(condition).as[A].rdd)
  }

  /**
    * Reads the dataset.
    *
    * @param descriptor The source descriptor
    * @tparam A The data type
    * @return
    */
  def readDataFrame[A: Encoder](descriptor: SingleFolderDataSourceDescriptor[A]): DataFrame = {
    datasourceManager.read(descriptor)
  }

  /**
    * Reads the dataset.
    *
    * @param descriptor The source descriptor
    * @param condition  The filter to apply
    * @tparam A The data type
    * @return
    */
  def readDataFrame[A: Encoder](descriptor: SingleFolderDataSourceDescriptor[A], condition: Column): DataFrame = {
    readDataFrame(descriptor).where(condition)
  }

  /**
    * Reads the latest folder for a dated dataset. It will return None when no folder is found
    *
    * @param descriptor The source descriptor
    * @tparam A The data type
    * @return
    */
  def readLatestDataFrame[A: Encoder](descriptor: DatedSourceDescriptor[A]): Option[DataFrame] = {
    val latestDate = listSourceDates(descriptor).lastOption
    latestDate.flatMap(datasourceManager.read(descriptor, _))
  }

  /**
    * Reads the data stored in the dataset for the given date. It will return None when the folder doesn't exist
    *
    * @param descriptor The source descriptor
    * @param date       The date
    * @tparam A The data type
    * @return
    */
  def readDataFrame[A: Encoder](descriptor: DatedSourceDescriptor[A], date: LocalDate): Option[DataFrame] = {
    if (config.horizon.forall(horizon => horizon.isAfter(date))) {
      datasourceManager.read(descriptor, date)
    } else {
      None
    }
  }

  /**
    * Reads the data stored in those folders between the given dates. It will return None when no folders are found
    *
    * @param descriptor The source descriptor
    * @param from       The min date (included)
    * @param to         The max date (excluded)
    * @tparam A The data type
    * @return
    */
  def readDataFrame[A: Encoder](descriptor: DatedSourceDescriptor[A], from: Option[LocalDate], to: Option[LocalDate]): Option[DataFrame] = {
    val dates = listSourceDates(descriptor).filter(date => from.forall(!date.isBefore(_)) && to.forall(date.isBefore(_)))
    datasourceManager.read(descriptor, dates)
  }

  /**
    * Import the implicits object to enable save operation on Datasets and Dataframes (it also imports spark.implicits._)
    */
  object implicits extends SQLImplicits with Serializable { // scalastyle:ignore

    // Necessary to implement SQLImplicits
    @transient override protected lazy val _sqlContext = spark.sqlContext

    implicit class DatasetOpts[A: Encoder](dataset: Dataset[A]) {

      /**
        * Save the Dataset to the given data source
        *
        * @param descriptor The data source descriptor
        */
      def save(descriptor: SingleFolderDataSourceDescriptor[A]): Unit = {
        save(descriptor, SaveMode.ErrorIfExists)
      }

      /**
        * Save the Dataset to the given data source
        *
        * @param descriptor The data source descriptor
        * @param saveMode   The spark save mode
        */
      def save(descriptor: SingleFolderDataSourceDescriptor[A], saveMode: SaveMode): Unit = {
        write(dataset.toDF(), descriptor, saveMode)
      }

      /**
        * Save the Dataset to the given data source
        *
        * @param descriptor The data source descriptor
        * @param date       The folder date
        */
      def save(descriptor: DatedSourceDescriptor[A], date: LocalDate): Unit = {
        save(descriptor, date, SaveMode.ErrorIfExists)
      }

      /**
        * Save the Dataset to the given data source
        *
        * @param descriptor The data source descriptor
        * @param date       The folder date
        * @param saveMode   The spark save mode
        */
      def save(descriptor: DatedSourceDescriptor[A], date: LocalDate, saveMode: SaveMode): Unit = {
        write(dataset.toDF(), descriptor, date, saveMode)
      }
    }

    implicit class DataframeOpts(dataframe: DataFrame) {

      /**
        * Save the DataFrame to the given data source
        *
        * @param descriptor The data source descriptor
        * @tparam A The data type
        */
      def save[A: Encoder](descriptor: SingleFolderDataSourceDescriptor[A]): Unit = {
        save(descriptor, SaveMode.ErrorIfExists)
      }

      /**
        * Save the DataFrame to the given data source
        *
        * @param descriptor The data source descriptor
        * @param saveMode   The spark save mode
        * @tparam A The data type
        */
      def save[A: Encoder](descriptor: SingleFolderDataSourceDescriptor[A], saveMode: SaveMode): Unit = {
        checkDataframeCompatibleWithT[A](dataframe) match {
          case Success(_) => write(dataframe, descriptor, saveMode)
          case Failure(t) => throw new RuntimeException(s"Dataset ${descriptor.name} is incompatible with dataframe schema", t)

        }
      }

      /**
        * Save the DataFrame to the given data source
        *
        * @param descriptor The data source descriptor
        * @param date       The folder date
        * @tparam A The data type
        */
      def save[A: Encoder](descriptor: DatedSourceDescriptor[A], date: LocalDate): Unit = {
        save(descriptor, date, SaveMode.ErrorIfExists)
      }

      /**
        * Save the DataFrame to the given data source
        *
        * @param descriptor The data source descriptor
        * @param date       The folder date
        * @param saveMode   The spark save mode
        * @tparam A The data type
        */
      def save[A: Encoder](descriptor: DatedSourceDescriptor[A], date: LocalDate, saveMode: SaveMode): Unit = {
        checkDataframeCompatibleWithT[A](dataframe) match {
          case Success(_) => write(dataframe, descriptor, date, saveMode)
          case Failure(t) => throw new RuntimeException(s"Dataset ${descriptor.name} is incompatible with dataframe schema", t)

        }
      }
    }

    implicit class RDDOpts[A: Encoder](rdd: RDD[A]) {

      /**
        * Save the RDD to the given data source
        *
        * @param descriptor The data source descriptor
        */
      def save(descriptor: SingleFolderDataSourceDescriptor[A]): Unit = {
        save(descriptor, SaveMode.ErrorIfExists)
      }

      /**
        * Save the RDD to the given data source
        *
        * @param descriptor The data source descriptor
        * @param saveMode   The spark save mode
        */
      def save(descriptor: SingleFolderDataSourceDescriptor[A], saveMode: SaveMode): Unit = {
        write(rdd.toDF(), descriptor, saveMode)
      }

      /**
        * Save the RDD to the given data source
        *
        * @param descriptor The data source descriptor
        * @param date       The folder date
        */
      def save(descriptor: DatedSourceDescriptor[A], date: LocalDate): Unit = {
        save(descriptor, date, SaveMode.ErrorIfExists)
      }

      /**
        * Save the RDD to the given data source
        *
        * @param descriptor The data source descriptor
        * @param date       The folder date
        * @param saveMode   The spark save mode
        */
      def save(descriptor: DatedSourceDescriptor[A], date: LocalDate, saveMode: SaveMode): Unit = {
        write(rdd.toDF(), descriptor, date, saveMode)
      }
    }

    /**
      * Writes a dataset
      *
      * @param dataframe  The dataset to write
      * @param descriptor The dataset info
      * @param saveMode   The spark save mode
      */
    private[this] def write[A: Encoder](
        dataframe: DataFrame,
        descriptor: DatedSourceDescriptor[A],
        date: LocalDate,
        saveMode: SaveMode
    ): Unit = {
      datasourceManager.write(dataframe, descriptor, date, saveMode)
    }

    /**
      * Writes a dataset
      *
      * @param dataframe  The dataset to write
      * @param descriptor The dataset info
      * @param saveMode   The spark save mode
      */
    private[this] def write[A: Encoder](
        dataframe: DataFrame,
        descriptor: SingleFolderDataSourceDescriptor[A],
        saveMode: SaveMode
    ): Unit = {
      datasourceManager.write(dataframe, descriptor, saveMode)
    }

    private def checkDataframeCompatibleWithT[A: Encoder](dataframe: DataFrame): Try[Unit] = {
      Try {
        dataframe.as[A]
        ()
      }
    }
  }

}

object PipelineContext {

  def apply(config: DatasourceConfig, spark: SparkSession): PipelineContext = {
    new PipelineContext(spark, DataSourceManager(config, spark), config)
  }
}

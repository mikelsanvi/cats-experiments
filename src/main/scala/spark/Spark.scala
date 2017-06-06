package spark

import org.apache.spark.sql.SparkSession

/**
  * Created by mikelsanvicente on 5/17/17.
  */
object Spark {

  private[this] var sparkSession: Option[SparkSession] = None

  def spark(): SparkSession = {
    val session = sparkSession.getOrElse {
      val builder = SparkSession
        .builder()
        .master("local[6]")
        .appName("Mappings")
        .config("spark.debug.maxToStringFields", 200)
        .config("mapreduce.fileoutputcommitter.algorithm.version", "2")

      builder.getOrCreate()
      // see http://stackoverflow.com/questions/36927918/using-spark-to-write-a-parquet-file-to-s3-over-s3a-is-very-slow
    }

    if (sparkSession.isEmpty)
      sparkSession = Some(session)

    session
  }

}


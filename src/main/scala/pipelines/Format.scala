
package pipelines

sealed abstract class Format

final case class JSON(compression: Compression) extends Format

final case class Parquet(compression: Compression) extends Format

trait Compression {
  def name: String
}
case object NoCompression extends Compression {
  def name: String = "none"
}
case object Gzip extends Compression {
  def name: String = "gzip"
}
case object Snappy extends Compression {
  def name: String = "snappy"
}
case object Lzo extends Compression {
  def name: String = "lzo"
}

object Format {

  /** Creates a JSON format object
    *
    * @param compression The compression algorithm used to store the data
    * @return
    */
  def json(compression: Compression = NoCompression): Format = {
    JSON(compression)
  }

  /** Creates a Parquet format object
    *
    * @param compression The compression algorithm used to store the data
    * @return
    */
  def parquet(compression: Compression = Snappy): Format = {
    Parquet(compression)
  }
}

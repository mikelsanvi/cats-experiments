package pipelines.utils

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

import scala.util.Try

private[pipelines] class HadoopUtils extends Serializable {

  /** List the `DatePath` under pathString containing a `_SUCCESS` file.
    *
    * The folder is suppose to have the following form:
    * pathString/yyyy-MM-dd/suffix/_SUCCESS
    *
    * Note: `suffix` can contain sub folders (just use `/`) but `suffix` can't start with a `/` character
    *
    * @param pathString the parent path
    * @param dateFormatter the format of the date in the URL
    * @param suffix second part of the path coming after the date; can contain `/`; should not start with `/`
    * @return a List of DatePath
    */
  def listDateFoldersWithSuccess(
      pathString: String,
      dateFormatter: DateTimeFormatter,
      suffix: Option[String] = None
  ): Seq[DatedFolder] = {
    listDir(pathString)
      .flatMap(p => Try(LocalDate.parse(p.getName, dateFormatter) -> p).toOption)
      .map { case (date, path) => (date, appendSuffix(path, suffix)) }
      .collect { case (date, path) if hasSuccessFile(path) => DatedFolder(date, path.toString) }
  }

  /**
    * Returns whether the given path contains a _SUCCESS file
    *
    * @param pathString The path to check
    * @return
    */
  def exists(pathString: String): Boolean = {
    val path = new Path(pathString)
    hasSuccessFile(path)
  }

  private def listDir(pathString: String): Seq[Path] = {
    val path = new Path(pathString)
    val fs = getFileSystem(path)
    val leaves = Option(fs.globStatus(path)) match {
      case Some(statusArray) => fs.listStatus(statusArray.map(_.getPath))
      case None => Array.empty[FileStatus]
    }
    leaves.collect { case leaf if leaf.isDirectory => leaf.getPath }.toList
  }

  private def getFileSystem(path: Path): FileSystem = {
    path.getFileSystem(new Configuration())
  }

  private def hasSuccessFile(parent: Path): Boolean = {
    val path = new Path(parent, "_SUCCESS")
    getFileSystem(path).exists(path)
  }

  /** Utility function for paths of the form `parent/2017-01-01/why/cant/we/have/nice/things`.
    *
    * @param parent the parent path
    * @param suffix the second part of the path, can contain `/` but it should not start with `/`
    * @return a Path extended with `suffix` if `suffix` is defined, otherwise `parent`
    */
  private def appendSuffix(parent: Path, suffix: Option[String]): Path =
    suffix.map(s => new Path(parent, s.stripPrefix("/"))).getOrElse(parent)

}

case class DatedFolder(date: LocalDate, path: String)

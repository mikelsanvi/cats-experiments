package implicits


import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

object Tests extends App {

  def readTree(id: String): Future[Option[String]] = Future{
    Some(id)
  }

  def readAllTrees(channels: List[String]): Future[Seq[String]] = {
    Future.sequence(channels.map(readTree(_))).map(_.flatten)
  }

  val channels = List[String]("dsf" , "sdf")

  println(Await.result(readAllTrees(channels), Duration.Inf))
}


case object MyTest
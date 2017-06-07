package free.pipelines

import org.apache.spark.sql.SparkSession

/**
  * Created by mikelsanvicente on 6/7/17.
  */
object Test extends App {
  def run[Container[_]](pipelineContext: PipelineContext[Container], source: Container[Person]): Unit = {
    import pipelineContext.syntax._
    val program = for {
      mikel <- pipelineContext.extract(source).mapping(p => Name(p.name)).filter(_.name == "mikel")
      mikelUpper <- mikel.mapping(n => n.copy(name = n.name.toUpperCase()))
      javier <- pipelineContext.extract(source).mapping(p => Name(p.name)).filter(_.name == "javier")
      res <- mikel ++ javier ++ mikelUpper
    } yield (res)

    println(program.load())
  }

  val input = List(Person("mikel", "san vicente"), Person("javier", "san vicente"))

  run(new PipelineContext(new SeqInterpreter()), input)
  val sparkSession = SparkSession.builder().master("local[6]").config("spark.ui.enabled", false).getOrCreate()
  import sparkSession.sqlContext.implicits._

  run(new PipelineContext(
    new SparkInterpreter(sparkSession)),
    sparkSession.sqlContext.createDataset(input))


}

case class Person(name: String, surname: String)

case class Name(name: String)
package spark

import org.apache.spark.sql.{Column, ColumnName, DataFrame, Dataset}
import shapeless._

/**
  * Created by mikelsanvicente on 5/15/17.
  */

object Syntax {
  implicit class DatasetOps[A](dataset: Dataset[A]) {
    def selectColumn[K](witness: Witness.Aux[K])(implicit mkLens: MkFieldLens[A, K]): DataFrame = {
      dataset.select(dataset.col(witness.value.asInstanceOf[Symbol].name))
    }

    def whereColumn[K](witness: Witness.Aux[K])( condition: Column => Column)(implicit mkLens: MkFieldLens[A, K]): Dataset[A] = {
      dataset.where(condition(dataset.col(witness.value.asInstanceOf[Symbol].name)))
    }
  }
}

class QueryBuilder[A](dataset: Dataset[A]) {
  private[this] var df: DataFrame = dataset.toDF()

  def select[K](witness: Witness.Aux[K])(implicit mkLens: MkFieldLens[A, K]): Unit = {
    df = df.select(dataset.col(witness.value.asInstanceOf[Symbol].name))
  }

  def where[K](witness: Witness.Aux[K])( condition: Column => Column)(implicit mkLens: MkFieldLens[A, K]): Unit = {
    df = df.where(condition(dataset.col(witness.value.asInstanceOf[Symbol].name)))
  }
}

case class Test(field1: Test2, field2: String)
case class Test2(field1: String, field2: String)

object Test extends App {
  val spark = Spark.spark()
  import Syntax._
  import spark.implicits._
  val ds = spark.createDataset(Seq(Test(Test2("value11", "value12"), "value2")))

  ds.select($"field1.field1", $"field1.field2").as[Test2].collect()

  val ds2 = ds.whereColumn('field2)(_ === "value2")
  val ds3 = ds2.selectColumn('field1)

  // println(ds3.as[Test2].collect.headOption)

}

package smelter

import shapeless._
import shapeless.labelled._

/** A type class for converting a type A to a List of Attribute.
  * The type A fields must follow the following rules
  *  - The field names must be the attribute name in lower case
  *  - The field type can be a String, Option[String] or Seq[String]
  *
  * @tparam A - the parsed type
  */
trait AttributeGenerator[A] {
  def toAttributes(record: A): List[Attribute]
}

object AttributeGenerator {

  private val EMPTY_FIELD_NAME = ""

  def apply[A](implicit attributeGenerator: AttributeGenerator[A]): AttributeGenerator[A] = attributeGenerator

  implicit val hnilGenerator: AttributeGenerator[HNil] = createAttributeGenerator[HNil](_ => Nil)

  implicit def hlistGenerator[H, K <: Symbol, T <: HList](
      implicit
      witness: Witness.Aux[K],
      hGenerator: AttributeGenerator[H],
      tGenerator: AttributeGenerator[T]
  ): AttributeGenerator[FieldType[K, H] :: T] = createAttributeGenerator { hlist =>
    hGenerator.toAttributes(hlist.head).map { attr =>
      attr.copy(field = witness.value.name.toUpperCase)
    } ::: tGenerator.toAttributes(hlist.tail)
  }

  implicit def seqGenerator[A](
      implicit
      stringConverter: StringConverter[A]
  ): AttributeGenerator[Seq[A]] = createAttributeGenerator { seq =>
    seq
      .map(v => new Attribute(EMPTY_FIELD_NAME, stringConverter.toString(v)))
      .toList
  }

  implicit def optGenerator[A](
      implicit
      stringConverter: StringConverter[A]
  ): AttributeGenerator[Option[A]] = createAttributeGenerator { opt =>
    opt
      .map(v => new Attribute(EMPTY_FIELD_NAME, stringConverter.toString(v)))
      .toList
  }

  implicit def aGenerator[A](
      implicit
      stringConverter: StringConverter[A]
  ): AttributeGenerator[A] = createAttributeGenerator { a =>
    List(new Attribute(EMPTY_FIELD_NAME, stringConverter.toString(a)))
  }

  implicit def genericGenerator[A <: Product, Repr <: HList](
      implicit gen: LabelledGeneric.Aux[A, Repr],
      reprGenerator: Lazy[AttributeGenerator[Repr]]
  ): AttributeGenerator[A] = createAttributeGenerator { record =>
    reprGenerator.value.toAttributes(gen.to(record))
  }

  private def createAttributeGenerator[A](toAttributesFunction: A => List[Attribute]): AttributeGenerator[A] = new AttributeGenerator[A] {
    override def toAttributes(record: A): List[Attribute] = toAttributesFunction(record)
  }

}

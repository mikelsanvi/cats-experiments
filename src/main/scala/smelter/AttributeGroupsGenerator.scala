package smelter

import shapeless._
import shapeless.labelled._

/** A type class for converting type A to a List of AttributeGroup.
  * The type A fields must follow the following rules
  *  - The field names must be the attribute group name in lower case
  *  - The field type can be a case class, Option[_] or Seq[_]
  *  - Type A field names must match with the Attribute names
  *
  * @tparam A - the (flat) data payload type
  */
trait AttributeGroupsGenerator[A] extends Serializable {
  def toAttributeGroup(row: A): List[AttributeGroup]
}

object AttributeGroupsGenerator {

  def apply[A](implicit smelterGenerator: AttributeGroupsGenerator[A]): AttributeGroupsGenerator[A] = smelterGenerator

  implicit val hnilGenerator: AttributeGroupsGenerator[HNil] = createAttributeGroupGenerator[HNil](_ => Nil)

  implicit def hlistGenerator[K <: Symbol, H <: Product, T <: HList](
      implicit
      witness: Witness.Aux[K],
      attributeGenerator: AttributeGenerator[H],
      tGenerator: AttributeGroupsGenerator[T]
  ): AttributeGroupsGenerator[FieldType[K, H] :: T] = createAttributeGroupGenerator { hlist =>
    AttributeGroup(witness.value.name.toUpperCase, attributeGenerator.toAttributes(hlist.head)) :: tGenerator.toAttributeGroup(hlist.tail)
  }

  implicit def hlistSeqGenerator[K <: Symbol, H <: Product, T <: HList](
      implicit
      witness: Witness.Aux[K],
      attributeGenerator: AttributeGenerator[H],
      tGenerator: AttributeGroupsGenerator[T]
  ): AttributeGroupsGenerator[FieldType[K, Seq[H]] :: T] = createAttributeGroupGenerator { hlist =>
    hlist.head.map { v =>
      AttributeGroup(witness.value.name.toUpperCase, attributeGenerator.toAttributes(v))
    }.toList ::: tGenerator.toAttributeGroup(hlist.tail)
  }

  implicit def hlistOptionGenerator[K <: Symbol, H <: Product, T <: HList](
      implicit
      witness: Witness.Aux[K],
      attributeGenerator: AttributeGenerator[H],
      tGenerator: AttributeGroupsGenerator[T]
  ): AttributeGroupsGenerator[FieldType[K, Option[H]] :: T] = createAttributeGroupGenerator { hlist =>
    hlist.head.map { v =>
      AttributeGroup(witness.value.name.toUpperCase, attributeGenerator.toAttributes(v))
    }.toList ::: tGenerator.toAttributeGroup(hlist.tail)
  }

  implicit def genericGenerator[A <: Product, Repr <: HList](
      implicit gen: LabelledGeneric.Aux[A, Repr],
      reprGenerator: Lazy[AttributeGroupsGenerator[Repr]]
  ): AttributeGroupsGenerator[A] = createAttributeGroupGenerator { record =>
    reprGenerator.value.toAttributeGroup(gen.to(record))
  }

  private def createSingleGroupGenerator[A: Manifest, L <: HList](groupType: String)(
      implicit
      attributeGenerator: AttributeGenerator[A]): AttributeGroupsGenerator[A] = createAttributeGroupGenerator { record =>
    List(AttributeGroup(groupType, attributeGenerator.toAttributes(record)))
  }

  private def createAttributeGroupGenerator[A](toAttributeGroupFunction: A => List[AttributeGroup]): AttributeGroupsGenerator[A] = {
    new AttributeGroupsGenerator[A] {
      override def toAttributeGroup(row: A): List[AttributeGroup] = toAttributeGroupFunction(row)
    }
  }
}

package smelter

import shapeless._
import shapeless.labelled._

/** A type class for converting List[AttributeGroup] to a type A.
  *
  * The type A fields must follow the following rules
  *  - The field name must be the attribute group name in lower case
  *  - The field type can be a case class, Option[_] or Seq[_]
  *  - All values in List[AttributeGroup] must have a matching field in A
  *
  * @tparam A - the (flat) data payload type
  */
trait AttributeGroupsExtractor[A] extends Serializable {
  def extract(attributeGroups: List[AttributeGroup]): A
}

object AttributeGroupsExtractor {

  /** Extractor for an empty HList.
    *
    * Will throw a RuntimeException if all the attributeGroups haven't been extracted.
    *
    */
  implicit val hnilExtractor: AttributeGroupsExtractor[HNil] = createAttributeGroupsExtractor[HNil] { attributeGroups =>
    if (attributeGroups.nonEmpty) {
      throw new RuntimeException(s"Some attribute groups can not be mapped to any field ${attributeGroups.map(_.groupType).mkString(", ")}")
    } else {
      HNil
    }
  }

  /** Extractor for a HList where the head is of type H.
    *
    * Will throw a RuntimeException attributeGroup.size != 1.
    *
    * @param witness The Symbol of the head.
    * @param tBuilder The extractor of the tail of the HList.
    * @param attributeExtractor AttributeExtractor[H]
    * @tparam K The type of the witness.
    * @tparam H The type of the HList head.
    * @tparam T The type of the HList tail.
    * @return An AttributeGroupsExtractor for a HList where the head is of type H.
    */
  implicit def hHlistExtractor[K <: Symbol, H <: Product, T <: HList](
      implicit
      witness: Witness.Aux[K],
      attributeExtractor: AttributeExtractor[H],
      tBuilder: AttributeGroupsExtractor[T]
  ): AttributeGroupsExtractor[FieldType[K, H] :: T] = createHListExtractor { attributeGroups =>
    attributeGroups match {
      case List(attributeGroup) =>
        attributeExtractor.extract(attributeGroup.attributes)
      case Nil =>
        throw new RuntimeException(s"Attribute group ${witness.value.name} not found")
      case _ =>
        throw new RuntimeException(s"Multiple attribute groups with name ${witness.value.name}")
    }
  }

  /** Extractor for a HList where the head is of type Seq[H].
    *
    * @param witness The Symbol of the head.
    * @param tBuilder The extractor of the tail of the HList.
    * @param attributeExtractor AttributeExtractor[H]
    * @tparam K The type of the witness.
    * @tparam H The type of the HList head.
    * @tparam T The type of the HList tail.
    * @return An AttributeGroupsExtractor for a HList where the head is of type Seq[H].
    */
  implicit def hlistSeqExtractor[K <: Symbol, H <: Product, T <: HList](
      implicit
      witness: Witness.Aux[K],
      attributeExtractor: AttributeExtractor[H],
      tBuilder: AttributeGroupsExtractor[T]
  ): AttributeGroupsExtractor[FieldType[K, Seq[H]] :: T] = createHListExtractor { attributeGroups =>
    attributeGroups.map(group => attributeExtractor.extract(group.attributes))
  }

  /** Extractor for a HList where the head is of type Option[H].
    *
    * Will throw a RuntimeException attributeGroup.size > 1.
    *
    * @param witness The Symbol of the head.
    * @param tBuilder The extractor of the tail of the HList.
    * @param attributeExtractor AttributeExtractor for the head of the HList
    * @tparam K The type of the witness.
    * @tparam H The type of the HList head.
    * @tparam T The type of the HList tail.
    * @return An AttributeGroupsExtractor for a HList where the head is of type Option[H].
    */
  implicit def hlistOptionExtractor[K <: Symbol, H <: Product, T <: HList](
      implicit
      witness: Witness.Aux[K],
      attributeExtractor: AttributeExtractor[H],
      tBuilder: AttributeGroupsExtractor[T]
  ): AttributeGroupsExtractor[FieldType[K, Option[H]] :: T] = createHListExtractor { attributeGroups =>
    if (attributeGroups.lengthCompare(1) > 0) {
      throw new RuntimeException(s"Multiple attribute groups with name ${witness.value.name}")
    } else {
      attributeGroups.headOption.map(group => attributeExtractor.extract(group.attributes))
    }
  }

  /** Extractor for a case class.
    *
    * This combines the different extractors (based on the types of the fields of A) to build an extractor for A.
    *
    * @param gen Converts from A to Repr (from case class to HList).
    * @param reprExtractor The AttributeGroupsExtractor of the Repr.
    * @tparam A The case class type.
    * @tparam Repr The representation of A as an HList
    * @return AttributeExtractor[A]
    */
  implicit def genericExtractor[A <: Product, Repr <: HList](
      implicit gen: LabelledGeneric.Aux[A, Repr],
      reprExtractor: Lazy[AttributeGroupsExtractor[Repr]]
  ): AttributeGroupsExtractor[A] = createAttributeGroupsExtractor { attributeGroups =>
    gen.from(reprExtractor.value.extract(attributeGroups))
  }

  /** Helper function to build a HList extractor.
    *
    * The AttributeGroupsExtractor is build recursively on the HList. This takes as argument the extractor for the head of the HList.
    *
    * @param headExtractor The extractor of the head of the HList
    * @param witness The Symbol of the head.
    * @param tExtractor The extractor of the tail of the HList.
    * @param attributeExtractor AttributeExtractor[H]
    * @tparam K The type of the witness.
    * @tparam T The type of the HList tail.
    * @tparam O The return type of the head extractor.
    * @return An AttributeGroupsExtractor for a HList where the head is of type O.
    */
  private def createHListExtractor[K <: Symbol, H, T <: HList, O](headExtractor: List[AttributeGroup] => O)(
      implicit
      witness: Witness.Aux[K],
      tExtractor: AttributeGroupsExtractor[T],
      attributeExtractor: AttributeExtractor[H]
  ): AttributeGroupsExtractor[FieldType[K, O] :: T] = createAttributeGroupsExtractor { attributeGroups =>
    val (groups, otherGroups) = attributeGroups.partition(_.groupType == witness.value.name.toUpperCase)
    field[K](headExtractor(groups)) :: tExtractor.extract(otherGroups)
  }

  /** Helper function to build single group extractor (for flat case class). */
  private def createSingleGroupExtractor[A, L <: HList](groupType: String)(
      implicit attributeExtractor: AttributeExtractor[A]
  ): AttributeGroupsExtractor[A] = createAttributeGroupsExtractor[A] { attributeGroups =>
    if (attributeGroups.lengthCompare(1) > 0) {
      throw new RuntimeException(s"Unexpected attribute groups ${attributeGroups.map(_.groupType)}")
    } else {
      attributeGroups.find(_.groupType == groupType) match {
        case Some(attributeGroup) =>
          attributeExtractor.extract(attributeGroup.attributes)
        case None =>
          throw new RuntimeException(s"Could not find expected attribute group $groupType: ${attributeGroups.map(_.groupType)}")
      }
    }
  }

  /** Smart constructor to build new AttributeGroupsExtractor. */
  private def createAttributeGroupsExtractor[A](fromAttributeGroups: List[AttributeGroup] => A): AttributeGroupsExtractor[A] = {
    new AttributeGroupsExtractor[A] {
      override def extract(attributeGroups: List[AttributeGroup]): A = fromAttributeGroups(attributeGroups)
    }
  }
}

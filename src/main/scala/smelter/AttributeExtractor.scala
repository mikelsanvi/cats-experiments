package smelter

import shapeless._
import shapeless.labelled._

import scala.util.Try

/** A type class for converting a List[Attribute] to a case class A.
  *
  * The fields of A must follow the following rules:
  *  - The field name must match the attribute name in lower case
  *  - The field type can be a String, Option[String] or Seq[String]
  *  - All values in List[Attribute] must have a matching field in A
  *
  * @tparam A - the parsed type
  */
trait AttributeExtractor[A] {
  def extract(attributes: List[Attribute]): A
}

object AttributeExtractor {

  def apply[A](implicit attributeExtractor: AttributeExtractor[A]): AttributeExtractor[A] = attributeExtractor

  implicit def seqExtractor[A](implicit stringConverter: StringConverter[A]): AttributeExtractor[Seq[A]] = createAttributeExtractor {
    attributes =>
      attributes.map { attr =>
        stringConverter.convert(attr.value) match {
          case Some(v) =>
            v
          case None =>
            throw new RuntimeException(s"Error parsing, ${attr.value} can not be casted to ${stringConverter.typeName}")
        }
      }
  }

  implicit def optExtractor[A](implicit stringConverter: StringConverter[A]): AttributeExtractor[Option[A]] = createAttributeExtractor {
    attributes =>
      if (attributes.lengthCompare(1) > 0) {
        throw new RuntimeException(s"Multiple values for field")
      } else {
        attributes.headOption.map { attr =>
          stringConverter.convert(attr.value) match {
            case Some(v) =>
              v
            case None =>
              throw new RuntimeException(s"Error parsing, ${attr.value} can not be casted to ${stringConverter.typeName}")
          }
        }
      }
  }

  implicit def aExtractor[A](implicit stringConverter: StringConverter[A]): AttributeExtractor[A] =
    createAttributeExtractor { attributes =>
      attributes match {
        case List(attribute) =>
          stringConverter.convert(attribute.value) match {
            case Some(v) =>
              v
            case None =>
              throw new RuntimeException(s"Error parsing, ${attribute.value} can not be casted to ${stringConverter.typeName}")
          }
        case Nil =>
          throw new RuntimeException(s"Could not find a value for field")
        case _ =>
          throw new RuntimeException(s"Multiple values for field")
      }
    }

  /** Extractor for an empty HList.
    *
    * Will throw a RuntimeException if all the attributes haven't been extracted.
    *
    */
  implicit val hnilExtractor: AttributeExtractor[HNil] = createAttributeExtractor[HNil] { attributes =>
    if (attributes.nonEmpty) {
      throw new RuntimeException(s"The following attributes could not be assigned to any field ${attributes.map(_.field).mkString(", ")}")
    } else {
      HNil
    }
  }

  /** Extractor for a HList where the head is a String.
    *
    * Will throw a RuntimeException if attributes.size != 1.
    *
    * @param witness    The Symbol of the head.
    * @param tExtractor The extractor of the tail of the HList.
    * @tparam K The type of the witness.
    * @tparam T The type of the HList tail.
    * @return An AttributeExtractor for a HList where the head is a String.
    */
  implicit def hlistExtractor[H, K <: Symbol, T <: HList](
      implicit
      witness: Witness.Aux[K],
      hExtractor: AttributeExtractor[H],
      tExtractor: AttributeExtractor[T]
  ): AttributeExtractor[FieldType[K, H] :: T] = createAttributeExtractor { attributes =>
    try {
      val (matchedAttributes, otherAttributes) = attributes.partition(_.field == witness.value.name.toUpperCase)
      field[K](hExtractor.extract(matchedAttributes)) :: tExtractor.extract(otherAttributes)
    } catch {
      case e: Throwable =>
        throw new RuntimeException(s"Error while extracting ${witness.value.name}: ${e.getMessage}", e)
    }
  }

  /** Extractor for a case class.
    *
    * This combines the different extractors (based on the types of the fields of A) to build an extractor for A.
    *
    * @param gen           Converts from A to Repr (from case class to HList).
    * @param reprExtractor The AttributeExtractor of Repr.
    * @tparam A    The case class type.
    * @tparam Repr The representation of A as an HList
    * @return AttributeExtractor[A]
    */
  implicit def genericExtractor[A <: Product, Repr <: HList](
      implicit gen: LabelledGeneric.Aux[A, Repr],
      reprExtractor: Lazy[AttributeExtractor[Repr]]
  ): AttributeExtractor[A] = createAttributeExtractor { attributes =>
    gen.from(reprExtractor.value.extract(attributes))
  }

  /** Smart constructor to build new AttributeExtractor. */
  private def createAttributeExtractor[A](fromAttributes: List[Attribute] => A): AttributeExtractor[A] = new AttributeExtractor[A] {
    override def extract(attributes: List[Attribute]): A = fromAttributes(attributes)
  }

}

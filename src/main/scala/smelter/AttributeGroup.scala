package smelter

case class AttributeGroup(
                           groupType: String,
                           attributes: List[Attribute]
                         ) {
  // Useful shortcut for extracting Fields. Note that this should not be used for fields which may appear multiple times in a group!
  def attrMap: Map[String, String] = attributes.map(attr => (attr.field, attr.value)).toMap

}

object AttributeGroup {

  implicit val ordering: Ordering[AttributeGroup] = Ordering.by(record => record.groupType -> record.attributes.sorted.toString())

}
case class Attribute(
                      field: String,
                      value: String
                    )

object Attribute {

  implicit val ordering: Ordering[Attribute] = Ordering.by(record => record.field -> record.value)

}

package smelter

trait SmelterBuilder[A] {
  def toSmelter(record: FlatSmelter[A]): Record

  def fromSmelter(record: Record): FlatSmelter[A]
}

object SmelterBuilder {

  def apply[A: AttributeGroupsExtractor: AttributeGroupsGenerator](): SmelterBuilder[A] = builder

  implicit def builder[A](
      implicit attributeGroupsExtractor: AttributeGroupsExtractor[A],
      attributeGroupsGenerator: AttributeGroupsGenerator[A]
  ): SmelterBuilder[A] = new SmelterBuilder[A] {
    override def fromSmelter(record: Record): FlatSmelter[A] = FlatSmelter(
      recordId = record.recordId,
      source = record.source,
      date = record.date,
      recordType = record.recordType,
      record = attributeGroupsExtractor.extract(record.attributeGroups)
    )

    override def toSmelter(record: FlatSmelter[A]): Record = Record(
      recordId = record.recordId,
      source = record.source,
      date = record.date,
      recordType = record.recordType,
      attributeGroups = attributeGroupsGenerator.toAttributeGroup(record.record)
    )
  }

  def toSmelter[A](record: FlatSmelter[A])(implicit smelterBuilder: SmelterBuilder[A]): Record =
    smelterBuilder.toSmelter(record)

  def fromSmelter[A](record: Record)(implicit smelterBuilder: SmelterBuilder[A]): FlatSmelter[A] =
    smelterBuilder.fromSmelter(record)
}

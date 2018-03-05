package smelter

case class Record(
                   recordId: String,
                   source: String,
                   date: String,
                   recordType: String,
                   attributeGroups: List[AttributeGroup]
                 )
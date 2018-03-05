package smelter

/** FlatSmelter is an improved version of Smelter Record, designed for optimization in storage and processing/
  * It uses an arbitrary type A to represent any (flat) data payload that may be contained in a smelter record and has a defined schema.
  *
  * Any case class of type A should have lower snake case field names for compatibility with the legacy smelter record field names.
  */
case class FlatSmelter[A](recordId: String, source: String, date: String, recordType: String, record: A)

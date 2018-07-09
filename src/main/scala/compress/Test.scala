package compress

import shapeless.Typeable

object Test extends App {
  
  case class Type1(field1: String, field2: Type2)
  case class Type2(field1: String, field2: Int)

  val encoder = CompressionEncoder[Type1]

  val compressed: encoder.Compressed  = encoder.compress(Seq(Type1("sdf", Type2("dsf", 34))))
  println(compressed)

  val seq = (1 to 10000).map {
    i =>
      Type1("name", if (i % 3 == 0) Type2("test", i % 2) else Type2("test1", 1))
  }

  val enc = CompressionEncoder[Type1]

  val res = enc.compress(seq)

  println(enc.decompress(res) == seq)

  println(Typeable[enc.Compressed].describe)

}

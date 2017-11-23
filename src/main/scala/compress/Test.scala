package compress

object Test extends App {

  val encoder = CompressionEncoder[Type1]

  val compressed: encoder.Compressed  = encoder.compress(Seq(Type1("sdf", Type2("dsf", 34))))
  println(compressed)
}

import shapeless._
import shapeless.ops.record.Selector

/**
  * Created by mikelsanvicente on 5/25/17.
  */
class RandomTests {
  case class Test(field1: Int, field2: String)

  def mapColumn[A, K, KV, Repr <: HList, Out](obj: A, witness: Witness.Aux[K], f: KV => Out)
                                             (implicit mkLens: MkFieldLens[A, K],
                                              gen: LabelledGeneric.Aux[A, Repr],
                                              fieldTypesRest: Selector.Aux[Repr, K, KV]): Out = ???


  mapColumn(Test(1, "hello"), 'field1, (v: Int) => v + 1) // this should retrieve 2
  mapColumn(Test(1, "hello"), 'field2, (v:String) => v + 1) // this should retrieve "hello1"
//  mapColumn(Test(1, "hello"), 'field2, (v: Int) => v + 1) // this should be a compilation error
//
}

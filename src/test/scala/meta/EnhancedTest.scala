package meta

import org.scalatest.FunSuite

class EnhancedTest extends FunSuite {

  test("expands enhanced") {
    val myType = MyType()
    assertCompiles("myType.field1")
    assertDoesNotCompile("myType.field2")
    assert(true)
  }

  test("expands check") {
    @meta.Check val myType =  (arg: MyType) => arg.field1

    assert(myType == "field1")

    assertDoesNotCompile("@meta.Check val myType =  if(true) 1")
    assertDoesNotCompile("@meta.Check val myType =  (arg: MyType) => 1")
  }
}
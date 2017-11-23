package meta

object InlineExample {
  final class C(val i: Int) {
    @inline def t2 = i*2
    @inline def t4 = t2*2
  }
  final class D(val i: Int) {
    def t2 = i*2
    def t4 = t2*2
  }
}

package free

import cats.free.Free

/**
  * Created by mikelsanvicente on 6/6/17.
  */
package object pipelines {
  type Extraction[Container[_], T] = Free[ExtractionDsl, Container[T]]
}

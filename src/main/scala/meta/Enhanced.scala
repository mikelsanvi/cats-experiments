package meta

import scala.annotation.StaticAnnotation
import scala.meta._

class Enhanced extends StaticAnnotation {
  inline def apply(defn: Any): Any = meta {
    defn match {
      case q"case class $tpname(...$paramss)" =>
        val fields = paramss.flatten.flatMap{
          param =>
            val duplicate =param.mods.exists{
              mod => mod.syntax == "@Drop"
            }
            if(duplicate) {
              None
            } else {
              Some(param)
            }
        }
        val name = Term.Name(tpname.value)
        val companion = q"object $name {  }"
        q"case class $tpname(..$fields); $companion"
    }
  }
}

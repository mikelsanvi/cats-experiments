package meta

import scala.annotation.StaticAnnotation
import scala.meta._

class Check extends StaticAnnotation {
  inline def apply(defn: Any): Any = meta {
    defn match {
      case q"val $name: $tpt = ($param: $ptpe) => $body" =>
        body match {
          case q"$param.$field" =>
            val fieldName = field.value
            val valName: Pat.Var.Term = Pat.Var.Term(Term.Name(name.syntax))
            q"val $valName: String = $fieldName"
          case _ =>
            abort(s"Funtion body doesn't match expected syntax `arg.field` ${body.syntax}")
        }
      case _ =>
        abort(s"Expression doesn't match expected syntax `val name = (arg: Type) => arg.field` ${defn.syntax}")
    }
  }
}

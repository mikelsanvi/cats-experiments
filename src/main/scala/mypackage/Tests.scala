package mypackage

import defaults.DefaultValue

object Tests extends App {
  val res = DefaultValue[String]

  println(res.default())
}

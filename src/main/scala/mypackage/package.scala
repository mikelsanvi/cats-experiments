import defaults.DefaultValue

package object mypackage {

  implicit val stringMyTypeClass = new DefaultValue[String] {
    override def default(): String = "default"
  }
}

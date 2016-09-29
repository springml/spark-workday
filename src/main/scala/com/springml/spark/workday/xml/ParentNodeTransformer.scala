package com.springml.spark.workday.xml

import scala.xml._
import scala.xml.transform._

/**
  * Created by sam on 28/9/16.
  */
class ParentNodeTransformer(
                           val parentNode : String,
                           val rt: RuleTransformer
                           ) extends RewriteRule {
  override def transform(n: Node): Seq[Node] = n match {
    case pn @ Elem(_, parentNode, _, _, _*) => rt(pn)
    case other => other
  }
}

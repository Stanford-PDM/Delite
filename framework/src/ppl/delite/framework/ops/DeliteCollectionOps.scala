package ppl.delite.framework.ops

import java.io.PrintWriter
import scala.reflect.SourceContext
import scala.virtualization.lms.common._
import scala.virtualization.lms.internal.{GenerationFailedException, GenericFatCodegen}
import ppl.delite.framework.datastructures.DeliteArray
import ppl.delite.framework.Interfaces

trait DeliteCollection[A]

trait DeliteCollectionOpsExp extends ExceptionOpsExp with BaseFatExp { this: DeliteOpsExp =>

  /**
   * Default implementations for delite collection operations
   */

  def dc_size[A:Manifest](x: Exp[DeliteCollection[A]])(implicit ctx: SourceContext) = x match {
    case Def(e: DeliteOpMap[_,_,_]) => e.size
    case Def(e: DeliteOpZipWith[_,_,_,_]) => e.size
    case _ => undefined("dc_size", x)
  }
  
  def dc_apply[A:Manifest](x: Exp[DeliteCollection[A]], n: Exp[Int])(implicit ctx: SourceContext): Exp[A] = {
    undefined("dc_apply", x)
  }

  // -- Used by OutputFlat, and OutputBuffer (but only on GPU)

  def dc_update[A:Manifest](x: Exp[DeliteCollection[A]], n: Exp[Int], y: Exp[A])(implicit ctx: SourceContext): Exp[Unit] = {
    undefined("dc_update", x)
  }

  // -- OutputBuffer methods

  /* Returns true if the collection can be used as a linear buffer for collect
   * elems (strategy OutputBuffer, need to implement the methods below).
   * Otherwise strategy OutputFlat has to be used, implement the method above.
   */
  def dc_linear_buffer[A:Manifest](x: Exp[DeliteCollection[A]])(implicit ctx: SourceContext): Boolean = {
    true
  }
   
  def dc_set_logical_size[A:Manifest](x: Exp[DeliteCollection[A]], y: Exp[Int])(implicit ctx: SourceContext): Exp[Unit] = {
    undefined("dc_alloc", x)
  }  
  
  /* returns true if the element y can be appended to collection x */
  def dc_appendable[A:Manifest](x: Exp[DeliteCollection[A]], i: Exp[Int], y: Exp[A])(implicit ctx: SourceContext): Exp[Boolean] = {
    undefined("dc_appendable", x)
  }
  
  /* append the element y to the collection x, returns unit */
  def dc_append[A:Manifest](x: Exp[DeliteCollection[A]], i: Exp[Int], y: Exp[A])(implicit ctx: SourceContext): Exp[Unit] = {
    undefined("dc_append", x)
  }

  def dc_alloc[A:Manifest,CA<:DeliteCollection[A]:Manifest](x: Exp[CA], size: Exp[Int])(implicit ctx: SourceContext): Exp[CA] = {
    undefined("dc_alloc", x)
  }  
  
  def dc_copy[A:Manifest](src: Exp[DeliteCollection[A]], srcPos: Exp[Int], dst: Exp[DeliteCollection[A]], dstPos: Exp[Int], size: Exp[Int])(implicit ctx: SourceContext): Exp[Unit] = {
    undefined("dc_copy", src)
  }   

  // - Struct transformation methods
  def dc_data_field(tp: Manifest[_]): String = ""

  def dc_size_field(tp: Manifest[_]): String = ""


  private def undefined[A](method: String, x: Exp[DeliteCollection[A]]) = {
    fatal(unit("no static implementation found for " + method + " on " + x.toString + " of type: " + x.tp))
  }

}

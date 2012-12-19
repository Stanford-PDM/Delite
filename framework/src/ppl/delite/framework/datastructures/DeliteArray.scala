package ppl.delite.framework.datastructures

import java.io.PrintWriter
import scala.virtualization.lms.common._
import scala.reflect.{SourceContext, RefinedManifest}
import scala.virtualization.lms.internal.{GenerationFailedException, GenericFatCodegen}
import scala.virtualization.lms.common._
import ppl.delite.framework.ops._
import ppl.delite.framework.Util._
import ppl.delite.framework.Config


trait DeliteArray[T] extends DeliteCollection[T] 

trait DeliteArrayOps extends StringOps {
  
  object DeliteArray {
    def apply[T:Manifest](length: Rep[Int])(implicit ctx: SourceContext) = darray_new(length)
    def imm[T:Manifest](length: Rep[Int])(implicit ctx: SourceContext) = darray_new_immutable(length)
  }

  implicit def repDArrayToDArrayOps[T:Manifest](da: Rep[DeliteArray[T]]) = new DeliteArrayOpsCls(da)

  class DeliteArrayOpsCls[T:Manifest](da: Rep[DeliteArray[T]]) {
    def length: Rep[Int] = darray_length(da)
    def apply(i: Rep[Int]): Rep[T] = darray_apply(da,i)
    def update(i: Rep[Int], x: Rep[T]): Rep[Unit] = darray_update(da,i,x)
    def map[B:Manifest](f: Rep[T] => Rep[B]) = darray_map(da,f)
    def zip[B:Manifest,R:Manifest](y: Rep[DeliteArray[B]])(f: (Rep[T],Rep[B]) => Rep[R]): Rep[DeliteArray[R]] = darray_zipwith(da,y,f)
    def reduce(f: (Rep[T],Rep[T]) => Rep[T], zero: Rep[T]): Rep[T] = darray_reduce(da,f,zero)
    def filter(f: Rep[T] => Rep[Boolean]) = darray_filter(da,f)
    def mkString(del: Rep[String]) = darray_mkstring(da,del)
    def union(rhs: Rep[DeliteArray[T]]) = darray_union(da,rhs)
    def intersect(rhs: Rep[DeliteArray[T]]) = darray_intersect(da,rhs)
    def take(n: Rep[Int]) = darray_take(da,n)
    def sort = darray_sort(da)
    def toSeq = darray_toseq(da)
  }
    
  implicit def darrayToString[A:Manifest](x: Rep[DeliteArray[A]]): Rep[String] = "[ " + repDArrayToDArrayOps(x).mkString(unit(" ")) + " ]"
  def infix_+[A:Manifest](lhs: String, rhs: Rep[DeliteArray[A]]) = string_plus(unit(lhs), darrayToString[A](rhs))
  
  def darray_new[T:Manifest](length: Rep[Int])(implicit ctx: SourceContext): Rep[DeliteArray[T]]
  def darray_new_immutable[T:Manifest](length: Rep[Int])(implicit ctx: SourceContext): Rep[DeliteArray[T]]
  def darray_length[T:Manifest](da: Rep[DeliteArray[T]])(implicit ctx: SourceContext): Rep[Int]
  def darray_apply[T:Manifest](da: Rep[DeliteArray[T]], i: Rep[Int])(implicit ctx: SourceContext): Rep[T]
  def darray_update[T:Manifest](da: Rep[DeliteArray[T]], i: Rep[Int], x: Rep[T])(implicit ctx: SourceContext): Rep[Unit]
  def darray_copy[T:Manifest](src: Rep[DeliteArray[T]], srcPos: Rep[Int], dest: Rep[DeliteArray[T]], destPos: Rep[Int], len: Rep[Int])(implicit ctx: SourceContext): Rep[Unit]
  def darray_map[A:Manifest,B:Manifest](a: Rep[DeliteArray[A]], f: Rep[A] => Rep[B]): Rep[DeliteArray[B]]    
  def darray_zipwith[A:Manifest,B:Manifest,R:Manifest](x: Rep[DeliteArray[A]], y: Rep[DeliteArray[B]], f: (Rep[A],Rep[B]) => Rep[R]): Rep[DeliteArray[R]]
  def darray_reduce[A:Manifest](x: Rep[DeliteArray[A]], f: (Rep[A],Rep[A]) => Rep[A], zero: Rep[A]): Rep[A]
  def darray_filter[A:Manifest](x: Rep[DeliteArray[A]], f: Rep[A] => Rep[Boolean]): Rep[DeliteArray[A]]
  def darray_mkstring[A:Manifest](a: Rep[DeliteArray[A]], del: Rep[String]): Rep[String]
  def darray_union[A:Manifest](lhs: Rep[DeliteArray[A]], rhs: Rep[DeliteArray[A]]): Rep[DeliteArray[A]]
  def darray_intersect[A:Manifest](lhs: Rep[DeliteArray[A]], rhs: Rep[DeliteArray[A]]): Rep[DeliteArray[A]]
  def darray_take[A:Manifest](lhs: Rep[DeliteArray[A]], n: Rep[Int])(implicit ctx: SourceContext): Rep[DeliteArray[A]]
  def darray_sort[A:Manifest](lhs: Rep[DeliteArray[A]]): Rep[DeliteArray[A]]
  def darray_range(st: Rep[Int], en: Rep[Int]): Rep[DeliteArray[Int]]
  def darray_toseq[A:Manifest](a: Rep[DeliteArray[A]]): Rep[Seq[A]]

  def darray_set_act_buf[A:Manifest](da: Rep[DeliteArray[A]]): Rep[Unit]
  def darray_set_act_final[A:Manifest](da: Rep[DeliteArray[A]]): Rep[Unit]
}

trait DeliteArrayCompilerOps extends DeliteArrayOps {
  def darray_unsafe_update[T:Manifest](x: Rep[DeliteArray[T]], n: Rep[Int], y: Rep[T])(implicit ctx: SourceContext): Rep[Unit]
  def darray_unsafe_copy[T:Manifest](src: Rep[DeliteArray[T]], srcPos: Rep[Int], dest: Rep[DeliteArray[T]], destPos: Rep[Int], len: Rep[Int])(implicit ctx: SourceContext): Rep[Unit]
}

trait DeliteArrayOpsExp extends DeliteArrayCompilerOps with DeliteArrayStructTags with DeliteCollectionOpsExp with DeliteStructsExp with EffectExp with PrimitiveOpsExp {
  this: DeliteOpsExp =>
  
  case class DeliteArrayNew[T:Manifest](length: Exp[Int]) extends DefWithManifest[T,DeliteArray[T]] 
  case class DeliteArrayLength[T:Manifest](da: Exp[DeliteArray[T]]) extends Def[Int]
  case class DeliteArrayApply[T:Manifest](da: Exp[DeliteArray[T]], i: Exp[Int]) extends DefWithManifest[T,T]
  case class DeliteArrayUpdate[T:Manifest](da: Exp[DeliteArray[T]], i: Exp[Int], x: Exp[T]) extends DefWithManifest[T,Unit]
  case class DeliteArrayCopy[T:Manifest](src: Exp[DeliteArray[T]], srcPos: Exp[Int], dest: Exp[DeliteArray[T]], destPos: Exp[Int], len: Exp[Int]) extends DefWithManifest[T,Unit]
  case class DeliteArrayMkString[T:Manifest](da: Exp[DeliteArray[T]], del: Exp[String]) extends DefWithManifest[T,String]
  case class DeliteArrayUnion[T:Manifest](lhs: Exp[DeliteArray[T]], rhs: Exp[DeliteArray[T]]) extends DefWithManifest[T,DeliteArray[T]]
  case class DeliteArrayIntersect[T:Manifest](lhs: Exp[DeliteArray[T]], rhs: Exp[DeliteArray[T]]) extends DefWithManifest[T,DeliteArray[T]]
  case class DeliteArrayTake[T:Manifest](lhs: Exp[DeliteArray[T]], n: Exp[Int]) extends DefWithManifest[T,DeliteArray[T]]
  case class DeliteArraySort[T:Manifest](da: Exp[DeliteArray[T]]) extends DefWithManifest[T,DeliteArray[T]]
  case class DeliteArrayRange(st: Exp[Int], en: Exp[Int]) extends Def[DeliteArray[Int]]
  case class DeliteArrayToSeq[A:Manifest](x: Exp[DeliteArray[A]]) extends Def[Seq[A]]

  //this is a hack to make writes to DeliteArray within a Struct an atomic operation in order to avoid creating mutable aliases
  //fortunately due to our limited data structure design this trick isn't required all over the place
  case class StructUpdate[T:Manifest](struct: Exp[Any], fields: List[String], i: Exp[Int], x: Exp[T]) extends DefWithManifest[T,Unit]
  case class VarUpdate[T:Manifest](v: Var[DeliteArray[T]], i: Exp[Int], x: Exp[T]) extends DefWithManifest[T,Unit]
  
  //this is a hack to make DeliteArray implement the buffer interface within Delite Ops without having to wrap the DeliteArray in a DeliteArrayBuffer
  case class DeliteArraySetActBuffer[T:Manifest](da: Exp[DeliteArray[T]]) extends DefWithManifest[T,Unit]
  case class DeliteArraySetActFinal[T:Manifest](da: Exp[DeliteArray[T]]) extends DefWithManifest[T,Unit]
  // switched because of a NoSuchMethodError problem when matching on the case object in other traits..
  // case object DeliteArrayGetActSize extends Def[Int]
  case class DeliteArrayGetActSize() extends Def[Int]

  //////////////////
  // delite ops
  
  case class DeliteArrayMap[A:Manifest,B:Manifest](in: Exp[DeliteArray[A]], func: Exp[A] => Exp[B])
    extends DeliteOpMap[A,B,DeliteArray[B]] {

    val size = copyTransformedOrElse(_.size)(in.length)
    override def alloc(len: Exp[Int]) = DeliteArray[B](len)
  }
  
  case class DeliteArrayZipWith[A:Manifest,B:Manifest,R:Manifest](inA: Exp[DeliteArray[A]], inB: Exp[DeliteArray[B]],
                                                                  func: (Exp[A], Exp[B]) => Exp[R])
    extends DeliteOpZipWith[A,B,R,DeliteArray[R]] {

    override def alloc(len: Exp[Int]) = DeliteArray[R](len)
    val size = copyTransformedOrElse(_.size)(inA.length)
  }
  
  case class DeliteArrayReduce[A:Manifest](in: Exp[DeliteArray[A]], func: (Exp[A], Exp[A]) => Exp[A], zero: Exp[A])
    extends DeliteOpReduce[A] {
    
    val size = copyTransformedOrElse(_.size)(in.length)    
  }  
  
  case class DeliteArrayMapFilter[A:Manifest,B:Manifest](in: Exp[DeliteArray[A]], func: Exp[A] => Exp[B], cond: Exp[A] => Exp[Boolean])
    extends DeliteOpFilter[A,B,DeliteArray[B]] {

    override def alloc(len: Exp[Int]) = DeliteArray[B](len)
    val size = copyTransformedOrElse(_.size)(in.length)
  }
  
   
  /////////////////////
  // delite collection
    
  def isDeliteArray[A](x: Exp[DeliteCollection[A]])(implicit ctx: SourceContext) = isSubtype(x.tp.erasure,classOf[DeliteArray[A]])  
  def asDeliteArray[A](x: Exp[DeliteCollection[A]])(implicit ctx: SourceContext) = x.asInstanceOf[Exp[DeliteArray[A]]]
    
  override def dc_size[A:Manifest](x: Exp[DeliteCollection[A]])(implicit ctx: SourceContext) = { 
    if (isDeliteArray(x)) asDeliteArray(x).length
    else super.dc_size(x)
  }
  
  override def dc_apply[A:Manifest](x: Exp[DeliteCollection[A]], n: Exp[Int])(implicit ctx: SourceContext) = {
    if (isDeliteArray(x)) asDeliteArray(x).apply(n)
    else super.dc_apply(x,n)
  }
  
  override def dc_update[A:Manifest](x: Exp[DeliteCollection[A]], n: Exp[Int], y: Exp[A])(implicit ctx: SourceContext) = {
    if (isDeliteArray(x)) asDeliteArray(x).update(n,y)
    else super.dc_update(x,n,y)        
  }

  override def dc_set_logical_size[A:Manifest](x: Exp[DeliteCollection[A]], y: Exp[Int])(implicit ctx: SourceContext) = {
    if (isDeliteArray(x)) {
      val arr = asDeliteArray(x)
      if (arr.length > y) { //trim
        val newArr = DeliteArray[A](y)
        darray_unsafe_copy(arr, unit(0), newArr, unit(0), y)
        darray_unsafe_set_act_final(newArr)
      }
      unit(())
    }
    else super.dc_set_logical_size(x,y)        
  }
  
  override def dc_append[A:Manifest](x: Exp[DeliteCollection[A]], i: Exp[Int], y: Exp[A])(implicit ctx: SourceContext) = {
    if (isDeliteArray(x)) {
      val arr = asDeliteArray(x)
      val size = darray_unsafe_get_act_size
      val length = arr.length
      if (size >= length) {
        val n = if (length < unit(16)) unit(16) else length*unit(2)
        val newArr = DeliteArray[A](n)
        darray_copy(arr, unit(0), newArr, unit(0), length)
        newArr(size) = y
        darray_unsafe_set_act_buf(newArr)
      }
      else {
        arr(size) = y
      }
      unit(true)
    }
    else super.dc_append(x,i,y)
  }
  
  override def dc_alloc[A:Manifest,CA<:DeliteCollection[A]:Manifest](x: Exp[CA], size: Exp[Int])(implicit ctx: SourceContext): Exp[CA] = {
    if (isDeliteArray(x)) DeliteArray[A](size).asInstanceOf[Exp[CA]]
    else super.dc_alloc[A,CA](x,size)
  } 
  
  override def dc_copy[A:Manifest](src: Exp[DeliteCollection[A]], srcPos: Exp[Int], dst: Exp[DeliteCollection[A]], dstPos: Exp[Int], size: Exp[Int])(implicit ctx: SourceContext): Exp[Unit] = {
    if (isDeliteArray(src) && isDeliteArray(dst)) {
      darray_unsafe_copy(asDeliteArray(src), srcPos, asDeliteArray(dst), dstPos, size)
    }
    else super.dc_copy(src,srcPos,dst,dstPos,size)
  }

  def darray_set_act_buf[A:Manifest](da: Exp[DeliteArray[A]]) = reflectEffect(DeliteArraySetActBuffer(da), Write(List(da.asInstanceOf[Sym[Any]])) andAlso Simple())
  def darray_set_act_final[A:Manifest](da: Exp[DeliteArray[A]]) = reflectEffect(DeliteArraySetActFinal(da), Write(List(da.asInstanceOf[Sym[Any]])) andAlso Simple())
  def darray_unsafe_set_act_buf[A:Manifest](da: Exp[DeliteArray[A]]) = reflectEffect(DeliteArraySetActBuffer(da))
  def darray_unsafe_set_act_final[A:Manifest](da: Exp[DeliteArray[A]]) = reflectEffect(DeliteArraySetActFinal(da))
  def darray_unsafe_get_act_size(): Exp[Int] = reflectEffect(DeliteArrayGetActSize())

  /* override def dc_parallelization[A:Manifest](x: Exp[DeliteCollection[A]], hasConditions: Boolean)(implicit ctx: SourceContext) = {
    if (isDeliteArray(x)) {
      if (hasConditions == true) throw new UnsupportedOperationException("DeliteArray: cannot have conditional Delite ops with a DeliteArray as input")
      ParFlat
    }
    else super.dc_parallelization(x, hasConditions)
  } */
  
    
  def darray_new[T:Manifest](length: Exp[Int])(implicit ctx: SourceContext) = reflectMutable(DeliteArrayNew[T](length))
  def darray_new_immutable[T:Manifest](length: Exp[Int])(implicit ctx: SourceContext) = reflectPure(DeliteArrayNew[T](length))
  def darray_length[T:Manifest](da: Exp[DeliteArray[T]])(implicit ctx: SourceContext) = reflectPure(DeliteArrayLength[T](da))
  def darray_apply[T:Manifest](da: Exp[DeliteArray[T]], i: Exp[Int])(implicit ctx: SourceContext) = reflectPure(DeliteArrayApply[T](da,i))
  
  /* 
   * rewrites to make ArrayUpdate operations atomic when the array is nested within another object (Variable, Struct)
   * these allow DSL authors to create data structures such as Var(Array), access them normally, and still work with the effects system
   * by preventing mutable aliases, i.e. preventing the compiler from every sharing a reference to anything but the outermost object   
   */
  def darray_update[T:Manifest](da: Exp[DeliteArray[T]], i: Exp[Int], x: Exp[T])(implicit ctx: SourceContext) = da match {
    case Def(Reflect(Field(struct,name),_,_)) => recurseFields(struct, List(name), i, x) //Struct(Array)
    case Def(Reflect(ReadVar(Variable(Def(Reflect(Field(struct,name),_,_)))),_,_)) => recurseFields(struct, List(name), i, x) //Struct(Var(Array))
    case Def(Reflect(ReadVar(v),_,_)) => reflectWrite(v.e)(VarUpdate[T](v,i,x)) //Var(Array)
    case _ => reflectWrite(da)(DeliteArrayUpdate[T](da,i,x))
  }

  private def recurseFields[T:Manifest](struct: Exp[Any], fields: List[String], i: Exp[Int], x: Exp[T]): Exp[Unit] = struct match {
    case Def(Reflect(Field(s,name),_,_)) =>recurseFields(s, name :: fields, i, x)
    case _ => reflectWrite(struct)(StructUpdate[T](struct, fields, i, x))
  }
  
  def darray_copy[T:Manifest](src: Exp[DeliteArray[T]], srcPos: Exp[Int], dest: Exp[DeliteArray[T]], destPos: Exp[Int], len: Exp[Int])(implicit ctx: SourceContext) = reflectWrite(dest)(DeliteArrayCopy(src,srcPos,dest,destPos,len))  
  def darray_map[A:Manifest,B:Manifest](a: Exp[DeliteArray[A]], f: Exp[A] => Exp[B]) = reflectPure(DeliteArrayMap(a,f))   
  def darray_zipwith[A:Manifest,B:Manifest,R:Manifest](x: Rep[DeliteArray[A]], y: Rep[DeliteArray[B]], f: (Rep[A],Rep[B]) => Rep[R]) = reflectPure(DeliteArrayZipWith(x,y,f))
  def darray_reduce[A:Manifest](x: Exp[DeliteArray[A]], f: (Exp[A],Exp[A]) => Exp[A], zero: Exp[A]) = reflectPure(DeliteArrayReduce(x,f,zero))
  def darray_filter[A:Manifest](x: Exp[DeliteArray[A]], f: Exp[A] => Exp[Boolean]) = darray_mapfilter(x, (e:Exp[A]) => e, f)
  def darray_mkstring[A:Manifest](a: Exp[DeliteArray[A]], del: Exp[String]) = reflectPure(DeliteArrayMkString(a,del))
  def darray_union[A:Manifest](lhs: Exp[DeliteArray[A]], rhs: Exp[DeliteArray[A]]) = reflectPure(DeliteArrayUnion(lhs,rhs))
  def darray_intersect[A:Manifest](lhs: Exp[DeliteArray[A]], rhs: Exp[DeliteArray[A]]) = reflectPure(DeliteArrayIntersect(lhs,rhs))
  def darray_take[A:Manifest](lhs: Exp[DeliteArray[A]], n: Exp[Int])(implicit ctx: SourceContext) = reflectPure(DeliteArrayTake(lhs,n))
  def darray_sort[A:Manifest](lhs: Exp[DeliteArray[A]]) = reflectPure(DeliteArraySort(lhs))
  def darray_range(st: Exp[Int], en: Exp[Int]) = reflectPure(DeliteArrayRange(st,en))
  def darray_mapfilter[A:Manifest,B:Manifest](lhs: Exp[DeliteArray[A]], map: Exp[A] => Exp[B], cond: Exp[A] => Exp[Boolean]) = reflectPure(DeliteArrayMapFilter(lhs,map,cond))
  def darray_toseq[A:Manifest](a: Exp[DeliteArray[A]]) = DeliteArrayToSeq(a)
  
  /////////////
  // internal
  
  def darray_unsafe_update[T:Manifest](x: Exp[DeliteArray[T]], n: Exp[Int], y: Exp[T])(implicit ctx: SourceContext) = DeliteArrayUpdate(x,n,y)
  def darray_unsafe_copy[T:Manifest](src: Exp[DeliteArray[T]], srcPos: Exp[Int], dest: Exp[DeliteArray[T]], destPos: Exp[Int], len: Exp[Int])(implicit ctx: SourceContext) = DeliteArrayCopy(src,srcPos,dest,destPos,len)  


  //////////////
  // mirroring

  override def mirror[A:Manifest](e: Def[A], f: Transformer)(implicit ctx: SourceContext): Exp[A] = {
    (e match {
      case SimpleStruct(SoaTag(tag, length), elems) => struct(SoaTag(tag, f(length)), elems map { case (k,v) => (k, f(v)) })      
      case DeliteArrayLength(a) => darray_length(f(a))
      case DeliteArrayApply(a,x) => darray_apply(f(a),f(x))
      case e@DeliteArrayNew(l) => darray_new_immutable(f(l))(e.mA,ctx)
      case e@DeliteArrayTake(a,x) => darray_take(f(a),f(x))(e.mA,ctx)
      case e@DeliteArraySort(x) => darray_sort(f(x))(e.mA)
      case e@DeliteArrayUpdate(l,i,r) => darray_unsafe_update(f(l),f(i),f(r))
      case e@DeliteArrayCopy(a,ap,d,dp,l) => toAtom(DeliteArrayCopy(f(a),f(ap),f(d),f(dp),f(l))(e.mA))(mtype(manifest[A]),implicitly[SourceContext])
      case e@DeliteArrayMap(in,g) => reflectPure(new { override val original = Some(f,e) } with DeliteArrayMap(f(in),f(g))(e.dmA,e.dmB))(mtype(manifest[A]),implicitly[SourceContext])      
      case e@DeliteArrayReduce(in,g,z) => reflectPure(new { override val original = Some(f,e) } with DeliteArrayReduce(f(in),f(g),f(z))(e.dmA))(mtype(manifest[A]),implicitly[SourceContext])
      case e@DeliteArrayMapFilter(in,g,c) => reflectPure(new { override val original = Some(f,e) } with DeliteArrayMapFilter(f(in),f(g),f(c))(e.dmA,e.dmB))(mtype(manifest[A]),implicitly[SourceContext])
      case Reflect(SimpleStruct(SoaTag(tag, length), elems), u, es) => reflectMirrored(Reflect(SimpleStruct(SoaTag(tag, f(length)), elems map { case (k,v) => (k, f(v)) }), mapOver(f,u), f(es)))(mtype(manifest[A]))
      case Reflect(e@DeliteArrayNew(l), u, es) => reflectMirrored(Reflect(DeliteArrayNew(f(l))(e.mA), mapOver(f,u), f(es)))(mtype(manifest[A]))
      case Reflect(DeliteArrayApply(l,r), u, es) => reflectMirrored(Reflect(DeliteArrayApply(f(l),f(r)), mapOver(f,u), f(es)))(mtype(manifest[A]))
      case Reflect(DeliteArrayLength(a), u, es) => reflectMirrored(Reflect(DeliteArrayLength(f(a)), mapOver(f,u), f(es)))(mtype(manifest[A]))
      case Reflect(DeliteArrayUpdate(l,i,r), u, es) => reflectMirrored(Reflect(DeliteArrayUpdate(f(l),f(i),f(r)), mapOver(f,u), f(es)))(mtype(manifest[A]))   
      case Reflect(StructUpdate(s,n,i,x), u, es) => reflectMirrored(Reflect(StructUpdate(f(s),n,f(i),f(x)), mapOver(f,u), f(es)))(mtype(manifest[A]))   
      case Reflect(VarUpdate(Variable(a),i,x), u, es) => reflectMirrored(Reflect(VarUpdate(Variable(f(a)),f(i),f(x)), mapOver(f,u), f(es)))(mtype(manifest[A]))  
      case Reflect(e@DeliteArrayTake(a,x), u, es) => reflectMirrored(Reflect(DeliteArrayTake(f(a),f(x))(e.mA), mapOver(f,u), f(es)))(mtype(manifest[A]))
      case Reflect(e@DeliteArraySort(x), u, es) => reflectMirrored(Reflect(DeliteArraySort(f(x))(e.mA), mapOver(f,u), f(es)))(mtype(manifest[A]))     
      case Reflect(e@DeliteArrayCopy(a,ap,d,dp,l), u, es) => reflectMirrored(Reflect(DeliteArrayCopy(f(a),f(ap),f(d),f(dp),f(l))(e.mA), mapOver(f,u), f(es)))(mtype(manifest[A]))     
      case Reflect(e@DeliteArrayMap(in,g), u, es) => reflectMirrored(Reflect(new { override val original = Some(f,e) } with DeliteArrayMap(f(in),f(g))(e.dmA,e.dmB), mapOver(f,u), f(es)))(mtype(manifest[A]))
      case Reflect(e@DeliteArrayReduce(in,g,z), u, es) => reflectMirrored(Reflect(new { override val original = Some(f,e) } with DeliteArrayReduce(f(in),f(g),f(z))(e.dmA), mapOver(f,u), f(es)))(mtype(manifest[A]))
      case Reflect(e@DeliteArrayMapFilter(in,g,c), u, es) => reflectMirrored(Reflect(new { override val original = Some(f,e) } with DeliteArrayMapFilter(f(in),f(g),f(c))(e.dmA,e.dmB), mapOver(f,u), f(es)))(mtype(manifest[A]))
      case Reflect(e@DeliteArrayGetActSize(), u, es) => reflectMirrored(Reflect(DeliteArrayGetActSize(), mapOver(f,u), f(es)))(mtype(manifest[A]))
      case Reflect(e@DeliteArraySetActBuffer(da), u, es) => reflectMirrored(Reflect(DeliteArraySetActBuffer(f(da))(e.mA), mapOver(f,u), f(es)))(mtype(manifest[A]))
      case Reflect(e@DeliteArraySetActFinal(da), u, es) => reflectMirrored(Reflect(DeliteArraySetActFinal(f(da))(e.mA), mapOver(f,u), f(es)))(mtype(manifest[A]))
      case _ => super.mirror(e,f)
    }).asInstanceOf[Exp[A]] // why??
  }
  
  override def syms(e: Any): List[Sym[Any]] = e match {
    case Def(SimpleStruct(SoaTag(tag, length), elems)) => syms(length) ++ super.syms(e)
    case _ => super.syms(e)
  }
  
  override def symsFreq(e: Any): List[(Sym[Any], Double)] = e match {
    case Def(SimpleStruct(SoaTag(tag, length), elems)) => symsFreq(length) ++ super.symsFreq(e)
    case _ => super.symsFreq(e)
  }  
  
  /////////////////////
  // aliases and sharing
  
  override def aliasSyms(e: Any): List[Sym[Any]] = e match {
    case DeliteArrayCopy(s,sp,d,dp,l) => Nil
    case _ => super.aliasSyms(e)
  }

  override def containSyms(e: Any): List[Sym[Any]] = e match {
    case NewVar(Def(Reflect(DeliteArrayNew(len),_,_))) => Nil  //ignore nested mutability for Var(Array): this is only safe because we rewrite mutations on Var(Array) to atomic operations
    case DeliteArrayCopy(s,sp,d,dp,l) => Nil
    case _ => super.containSyms(e)
  }

  override def extractSyms(e: Any): List[Sym[Any]] = e match {
    case DeliteArrayCopy(s,sp,d,dp,l) => Nil
    case _ => super.extractSyms(e)
  }

  override def copySyms(e: Any): List[Sym[Any]] = e match {
    case DeliteArrayCopy(s,sp,d,dp,l) => Nil // ??
    case _ => super.copySyms(e)
  }    
  
}

trait DeliteArrayStructTags extends Base with StructTags {
  case class SoaTag[T,DA <: DeliteArray[T]](base: StructTag[T], length: Rep[Int]) extends StructTag[DA]
}

trait DeliteArrayOpsExpOpt extends DeliteArrayOpsExp with DeliteArrayStructTags with StructExpOptCommon with DeliteStructsExp {
  this: DeliteOpsExp =>

  object StructIR {
    def unapply[A](e: Exp[DeliteArray[A]]): Option[(StructTag[A], Exp[Int], Seq[(String,Exp[DeliteArray[Any]])])] = e match {
      case Def(Struct(SoaTag(tag: StructTag[A],len),elems:Seq[(String,Exp[DeliteArray[Any]])])) => Some((tag,len,elems))
      case Def(Reflect(Struct(SoaTag(tag: StructTag[A],len), elems:Seq[(String,Exp[DeliteArray[Any]])]), u, es)) => Some((tag,len,elems))
      case _ => None
    }
  }

  object StructType { //TODO: we should have a unified way of handling this, e.g., TypeTag[T] instead of Manifest[T]
    def unapply[T:Manifest](e: Exp[DeliteArray[T]]) = if (Config.soaEnabled) unapplyStructType[T] else None
    def unapply[T:Manifest] = if (Config.soaEnabled) unapplyStructType[T] else None
  }

  //choosing the length of the first array creates an unnecessary dependency (all arrays must have same length), so we store length in the tag
  override def darray_length[T:Manifest](da: Exp[DeliteArray[T]])(implicit ctx: SourceContext) = da match {
    case Def(Loop(size,_,b:DeliteCollectElem[_,_,_])) if b.cond == Nil => size
    case StructIR(tag, len, elems) => 
      println("**** extracted array length: " + len.toString)
      len
    case StructType(tag, fields) =>
      val z = dlength(field(da,fields(0)._1)(mtype(darrayManifest(fields(0)._2)),ctx))(mtype(fields(0)._2),ctx)
      println("**** fallback array length: " + z + " of " + da.toString)
      z
    case _ => super.darray_length(da)
  }

  override def darray_apply[T:Manifest](da: Exp[DeliteArray[T]], i: Exp[Int])(implicit ctx: SourceContext) = da match {
    case StructIR(tag, len, elems) =>
      struct[T](tag, elems.map(p=>(p._1, darray_apply(p._2,i)(argManifest(p._2.tp),ctx))))
    case StructType(tag, fields) =>
      struct[T](tag, fields.map(p=>(p._1, darray_apply(field(da,p._1)(mtype(darrayManifest(p._2)),ctx),i)(mtype(p._2),ctx))))
    case _ => super.darray_apply(da, i)
  }

  //x more likely to match as a Struct than da?
  override def darray_update[T:Manifest](da: Exp[DeliteArray[T]], i: Exp[Int], x: Exp[T])(implicit ctx: SourceContext) = da match {
    case StructIR(tag, len, elems) =>
      elems.foreach(p=>darray_update(p._2,i,field(x,p._1)(argManifest(p._2.tp),ctx))(argManifest(p._2.tp),ctx))
    case StructType(tag, fields) =>
      fields.foreach(p=>darray_update(field(da,p._1)(mtype(darrayManifest(p._2)),ctx), i, field(x,p._1)(mtype(p._2),ctx))(mtype(p._2),ctx))
    case _ => super.darray_update(da, i, x)
  }

  override def darray_unsafe_update[T:Manifest](da: Exp[DeliteArray[T]], i: Exp[Int], x: Exp[T])(implicit ctx: SourceContext) = da match {
    case StructIR(tag, len, elems) =>
      elems.foreach(p=>darray_unsafe_update(p._2,i,field(x,p._1)(argManifest(p._2.tp),ctx))(argManifest(p._2.tp),ctx))
    case StructType(tag, fields) =>
      fields.foreach(p=>darray_unsafe_update(field(da,p._1)(mtype(darrayManifest(p._2)),ctx), i, field(x,p._1)(mtype(p._2),ctx))(mtype(p._2),ctx))
    case _ => super.darray_unsafe_update(da, i, x)
  }

  override def darray_copy[T:Manifest](src: Exp[DeliteArray[T]], srcPos: Exp[Int], dest: Exp[DeliteArray[T]], destPos: Exp[Int], length: Exp[Int])(implicit ctx: SourceContext) = dest match {
    case StructIR(tag, _, elems) =>
      elems.foreach{ case (k,v) => darray_copy(field(src,k)(v.tp,ctx), srcPos, v, destPos, length)(argManifest(v.tp),ctx) }
    case StructType(tag, fields) =>
      fields.foreach{ case (k,tp) => darray_copy(field(src,k)(mtype(darrayManifest(tp)),ctx), srcPos, field(dest,k)(mtype(darrayManifest(tp)),ctx), destPos, length)(mtype(tp),ctx) }
    case _ => super.darray_copy(src, srcPos, dest, destPos, length)
  }

  override def darray_unsafe_copy[T:Manifest](src: Exp[DeliteArray[T]], srcPos: Exp[Int], dest: Exp[DeliteArray[T]], destPos: Exp[Int], length: Exp[Int])(implicit ctx: SourceContext) = dest match {
    case StructIR(tag, _, elems) =>
      elems.foreach{ case (k,v) => darray_unsafe_copy(field(src,k)(v.tp,ctx), srcPos, v, destPos, length)(argManifest(v.tp),ctx) }
    case StructType(tag, fields) =>
      fields.foreach{ case (k,tp) => darray_unsafe_copy(field(src,k)(mtype(darrayManifest(tp)),ctx), srcPos, field(dest,k)(mtype(darrayManifest(tp)),ctx), destPos, length)(mtype(tp),ctx) }
    case _ => super.darray_unsafe_copy(src, srcPos, dest, destPos, length)
  }

  override def darray_take[T:Manifest](da: Exp[DeliteArray[T]], n: Rep[Int])(implicit ctx: SourceContext) = da match {
    case StructIR(tag, _, elems) =>
      struct[DeliteArray[T]](SoaTag(tag, n), elems.map(p=>(p._1, darray_take(p._2,n)(argManifest(p._2.tp),ctx))))
    case StructType(tag, fields) =>
      struct[DeliteArray[T]](SoaTag(tag, n), fields.map(p=>(p._1, darray_take(field(da,p._1)(mtype(darrayManifest(p._2)),ctx),n)(mtype(p._2),ctx))))
    case _ => super.darray_take(da, n)
  }



  private def argManifest[A,B](m: Manifest[A]): Manifest[B] = m.typeArguments(0).asInstanceOf[Manifest[B]]

  //forwarder to appease type-checker
  private def dnew[T:Manifest](length: Exp[Int])(implicit ctx: SourceContext): Exp[DeliteArray[T]] = darray_new(length)
  private def dnewi[T:Manifest](length: Exp[Int])(implicit ctx: SourceContext): Exp[DeliteArray[T]] = darray_new_immutable(length)
  private def dlength[T:Manifest](da: Exp[DeliteArray[T]])(implicit ctx: SourceContext): Exp[Int] = darray_length(da)

  //TODO: if T <: Record, but no RefinedManifest -- how do we map the fields? reflection?
  override def darray_new[T:Manifest](length: Exp[Int])(implicit ctx: SourceContext) = manifest[T] match {
    case StructType(tag,fields) => 
      struct[DeliteArray[T]](SoaTag(tag,length), fields.map(p=>(p._1,dnew(length)(p._2,ctx))))
    case _ => super.darray_new(length)
  }

  override def darray_new_immutable[T:Manifest](length: Exp[Int])(implicit ctx: SourceContext) = manifest[T] match {
    case StructType(tag,fields) => 
      struct[DeliteArray[T]](SoaTag(tag,length), fields.map(p=>(p._1,dnewi(length)(p._2,ctx))))
    case _ => super.darray_new_immutable(length)
  }

  def darrayManifest(arg: Manifest[_]) = new Manifest[DeliteArray[_]] {
    val erasure = classOf[DeliteArray[_]]
    override val typeArguments = List(arg)
  }

  def deliteArrayPure[T:Manifest](da: Exp[DeliteArray[T]], elems: RefinedManifest[T])(implicit ctx: SourceContext): Exp[DeliteArray[T]] = {
    if (Config.soaEnabled)
      struct[DeliteArray[T]](SoaTag(AnonTag(elems),da.length), elems.fields.map(e=>(e._1, field[DeliteArray[_]](da,e._1)(darrayManifest(e._2),ctx))))
    else
      da
  }

  override def containSyms(e: Any): List[Sym[Any]] = e match {
    case NewVar(Def(Reflect(Struct(tag,_),_,_))) if tag.isInstanceOf[SoaTag[_,_]] => Nil //as above for SoA array
    case _ => super.containSyms(e)
  }

  override def unapplyStructType[T:Manifest]: Option[(StructTag[T], List[(String,Manifest[_])])] = manifest[T] match {
    case d if d.erasure == classOf[DeliteArray[_]] =>
      val elems = unapplyStructType(d.typeArguments(0))
      elems.map { case (tag: StructTag[T],fields) => (tag, fields.map(e => (e._1, darrayManifest(e._2)))) }
    case _ => super.unapplyStructType
  }
}

trait DeliteArrayFatExp extends DeliteArrayOpsExpOpt with StructFatExpOptCommon {
  this: DeliteOpsExp =>
}

trait BaseGenDeliteArrayOps extends GenericFatCodegen {
  val IR: DeliteArrayFatExp with DeliteOpsExp
  import IR._
  
  override def unapplySimpleIndex(e: Def[Any]): Option[(Exp[Any], Exp[Int])] = e match {
    case DeliteArrayApply(da, idx) => Some((da,idx))
    case _ => super.unapplySimpleIndex(e)
  }

  override def unapplySimpleDomain(e: Def[Int]): Option[Exp[Any]] = e match {
    //case DeliteArrayLength(da) => Some(da)
    case DeliteArrayLength(a /*@ Def(Loop(_,_,_:DeliteCollectElem[_,_,_]))*/) => Some(a) // exclude hash collect (?)
    case _ => super.unapplySimpleDomain(e)
  }

}

trait ScalaGenDeliteArrayOps extends BaseGenDeliteArrayOps with ScalaGenDeliteStruct with ScalaGenDeliteOps {
  val IR: DeliteArrayFatExp with DeliteOpsExp
  import IR._
  
  override def emitNode(sym: Sym[Any], rhs: Def[Any]) = rhs match {
    case a@DeliteArrayNew(n) => emitValDef(sym, "new Array[" + remap(a.mA) + "](" + quote(n) + ")")
    case DeliteArrayLength(da) =>
      emitValDef(sym, quote(da) + ".length //" + quotePos(sym))
    case DeliteArrayApply(da, idx) =>
      emitValDef(sym, quote(da) + "(" + quote(idx) + ")")
    case DeliteArrayUpdate(da, idx, x) =>
      emitValDef(sym, quote(da) + "(" + quote(idx) + ") = " + quote(x))
    case DeliteArrayCopy(src,srcPos,dest,destPos,len) =>
      emitValDef(sym, "System.arraycopy(" + quote(src) + "," + quote(srcPos) + "," + quote(dest) + "," + quote(destPos) + "," + quote(len) + ")")
    case DeliteArrayMkString(da,x) =>
      emitValDef(sym, quote(da) + ".mkString(" + quote(x) + ")")
    case DeliteArrayUnion(lhs,rhs) =>
      emitValDef(sym, quote(lhs) + " union " + quote(rhs))
    case DeliteArrayIntersect(lhs,rhs) =>
      emitValDef(sym, quote(lhs) + " intersect " + quote(rhs))    
    case DeliteArrayTake(lhs,n) =>
      emitValDef(sym, quote(lhs) + ".take(" + quote(n) + ")")
    case a@DeliteArraySort(x) => 
      stream.println("val " + quote(sym) + " = {")
      stream.println("val d = new Array[" + remap(a.mA) + "](" + quote(x) + ".length" + ")")
      stream.println("System.arraycopy(" + quote(x) + ", 0, d, 0, " + quote(x) + ".length)")
      stream.println("scala.util.Sorting.quickSort(d)")
      stream.println("d")
      stream.println("}")    
    case DeliteArrayRange(st,en) =>
      emitValDef(sym, "Array.range(" + quote(st) + "," + quote(en) + ")")
    case DeliteArrayToSeq(a) => emitValDef(sym, quote(a) + ".toSeq")
    case StructUpdate(struct, fields, idx, x) =>
      emitValDef(sym, quote(struct) + "." + fields.reduceLeft(_ + "." + _) + "(" + quote(idx) + ") = " + quote(x))
    case VarUpdate(Variable(a), idx, x) =>
      val readVar = if (deliteInputs contains a) ".get" else ""
      emitValDef(sym, quote(a) + readVar + "(" + quote(idx) + ") = " + quote(x))
    case DeliteArrayGetActSize() =>
      emitValDef(sym, getActSize)
    case DeliteArraySetActBuffer(da) =>
      emitValDef(sym, getActBuffer + " = " + quote(da))
    case DeliteArraySetActFinal(da) =>
      emitValDef(sym, getActFinal + " = " + quote(da))
    case _ => super.emitNode(sym, rhs)
  }

  override def remap[A](m: Manifest[A]): String = m.erasure.getSimpleName match {
    case "DeliteArray" => m.typeArguments(0) match {
      case StructType(_,_) => structName(m)
      case s if s <:< manifest[Record] => structName(m) // occurs due to restaging
      case arg => 
        "Array[" + remap(arg) + "]"
    }
    case _ => super.remap(m)
  }

}


trait CudaGenDeliteArrayOps extends BaseGenDeliteArrayOps with CudaGenFat {
  val IR: DeliteArrayFatExp with DeliteOpsExp
  import IR._

  override def emitNode(sym: Sym[Any], rhs: Def[Any]) = rhs match {
    //case DeliteArrayNew(length) =>
    //  emitValDef(sym, "new Array[" + remap(sym.tp.tpArguments(0)) + "](" + quote(length) + ")")
    case DeliteArrayLength(da) =>
      emitValDef(sym, quote(da) + ".length")
    case DeliteArrayApply(da, idx) =>
      emitValDef(sym, quote(da) + ".apply(" + quote(idx) + ")")
    case DeliteArrayUpdate(da, idx, x) =>
      emitValDef(sym, quote(da) + ".update(" + quote(idx) + "," + quote(x) + ");")
    case _ => super.emitNode(sym, rhs)
  }

  override def remap[A](m: Manifest[A]): String = m.erasure.getSimpleName match {
    case "DeliteArray" => m.typeArguments(0) match {
      case s if s <:< manifest[Record] =>
        throw new GenerationFailedException("CudaGen: Struct generation not possible")
      case arg => "DeliteArray<" + remap(arg) + ">"
    }
    case _ => super.remap(m)
  }

  override def isObjectType[A](m: Manifest[A]) : Boolean = m.erasure.getSimpleName match {
      case "DeliteArray" => m.typeArguments(0) match {
        case s if s <:< manifest[Record] => false
        case arg => true //if isPrimitiveType(arg) => true //Currently only allow primitive type arrays
      }
      case _ => super.isObjectType(m)
  }

}

trait OpenCLGenDeliteArrayOps extends BaseGenDeliteArrayOps with OpenCLGenFat {
  val IR: DeliteArrayFatExp with DeliteOpsExp
  import IR._

  override def emitNode(sym: Sym[Any], rhs: Def[Any]) = rhs match {
    //case DeliteArrayNew(length) =>
    //  emitValDef(sym, "new Array[" + remap(sym.tp.typeArguments(0)) + "](" + quote(length) + ")")
    case DeliteArrayLength(da) =>
      emitValDef(sym, remap(da.tp) + "_size(" + quote(da) + ")")
    case DeliteArrayApply(da, idx) =>
      emitValDef(sym, remap(da.tp) + "_apply(" + quote(da) + "," + quote(idx) + ")")
    case DeliteArrayUpdate(da, idx, x) =>
      emitValDef(sym, remap(da.tp) + "_update(" + quote(da) + "," + quote(idx) + "," + quote(x) + ")")
    case _ => super.emitNode(sym, rhs)
  }

  override def remap[A](m: Manifest[A]): String = m.erasure.getSimpleName match {
    case "DeliteArray" => m.typeArguments(0) match {
      case s if s <:< manifest[Record] =>
        throw new GenerationFailedException("OpenCLGen: Struct generation not possible")
      case arg => "DeliteArray_" + remap(arg)
    }
    case _ => super.remap(m)
  }

  override def isObjectType[A](m: Manifest[A]) : Boolean = m.erasure.getSimpleName match {
      case "DeliteArray" => m.typeArguments(0) match {
        case s if s <:< manifest[Record] => false
        case arg => true //if isPrimitiveType(arg) => true //Currently only allow primitive type arrays
      }
      case _ => super.isObjectType(m)
  }

}

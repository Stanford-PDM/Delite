package ppl.delite.framework.transform

import ppl.delite.framework.ops._
import ppl.delite.framework.datastructures._
import ppl.delite.framework.{Config, DeliteApplication}
import ppl.delite.framework.Util._
import scala.virtualization.lms.common._
import scala.reflect.SourceContext


//NOTE: unwrapping reduces isn't always safe (rFunc may not be separable); only for use in DSLs with appropriate restrictions
trait MultiloopSoATransformWithReduceExp extends MultiloopSoATransformExp {

  //TODO: merge this into standard SoA transform and check safety
  override def transformLoop(stm: Stm): Option[Exp[Any]] = stm match {
 //   case TP(sym, r:DeliteOpReduceLike[_]) if r.mutable => None // mutable reduces don't work yet
 //   case TP(sym, Loop(size, v, body: DeliteReduceElem[a])) => soaReduce[a](size,v,body)(body.mA)
    case TP(sym, Loop(size, v, body: DeliteHashReduceElem[k,v,i,cv])) => soaHashReduce[k,v,i,cv](size,v,body)(body.mK,body.mV,body.mI,body.mCV)
    case _ => super.transformLoop(stm)
  }

}

/**
  * The idea behind the soa transformer is just to replace a map over a struct type (say a vector) 
  * with a map over an array and then create the vector at the end after mapping. 
  * For example:
  *   vector.map(f) -> Vector(vector.getArray.map(f))
  * which allows the struct field bypass rewrites in lms to dce all the vector wrappers
  *
  * If the element type of the map is a struct, it splits the loop into a loop per field
  * the dce engine kills off unused fields and then the remaining fields are fused back 
  * together in the trait at the bottom of the file (augments Tiark's fusion algorithm) // TODO DAMIEN update this
  */

trait MultiloopSoATransformExp extends DeliteTransform with LoweringTransform with DeliteApplication
  with DeliteOpsExp with DeliteArrayFatExp { self =>

  private val t = new ForwardPassTransformer { // TODO: DAMIEN: give a better name to this
    val IR: self.type = self
    override def transformStm(stm: Stm): Exp[Any] = transformLoop(stm) match {
      case Some(newSym) => newSym
      case None => super.transformStm(stm)
    }

    def replace[A](oldExp: Exp[A], newExp: Exp[A]) {
      subst += this(oldExp) -> newExp
      printlog("replacing " + oldExp.toString + " with " + newExp.toString)
    }
  }

  def transformLoop(stm: Stm): Option[Exp[Any]] = stm match {
    case TP(sym, Loop(size, v, body: DeliteCollectElem[a,i,ca])) =>
      val pos: SourceContext = if (sym.pos.length > 0) sym.pos(0) else null
      soaCollect[a,i,ca](size,v,body)(body.mA,body.mI,body.mCA,pos) match {
        case s@Some(newSym) => stm match {
          case TP(sym, z:DeliteOpZipWith[_,_,_,_]) if(Config.enableGPUObjReduce) => //TODO: remove this
            encounteredZipWith += newSym -> z
            printdbg("Register DeliteOpZipWith symbol: " + z.toString + " -> " + newSym.toString)
            s
          case _ => s
        }
        case None => None
      }
    //TODO: unwrap SingleTask / Composite nodes to expose the struct return type (have to handle Reifys)
    //case TP(sym, s: DeliteOpSingleTask[r]) => unwrapSingleTask(s)(s.mR)
    case _ => None
  }

  if (Config.soaEnabled) {
    appendTransformer(t) // AoS to SoA should go last, right before fusion
  }

  private object StructBlock {
    def unapply[A](d: Block[A]): Option[(StructTag[A], Seq[(String, Rep[Any])])] = d match {
      case Block(Def(Struct(tag:StructTag[A],elems))) => Some((tag,elems))
      case Block(Def(Reify(Def(Struct(tag:StructTag[A],elems)),_,_))) => Some((tag,elems))
      case Block(Def(Reify(Def(Reflect(Struct(tag: StructTag[A], elems),_,_)),_,_))) => Some((tag,elems))
      case _ => None
    }
  }

  //collect elems: unwrap outer struct if return type is a Struct && perform SoA transform if element type is a Struct
  def soaCollect[A:Manifest, I<:DeliteCollection[A]:Manifest, CA<:DeliteCollection[A]:Manifest]
      (size: Exp[Int], v: Sym[Int], body: DeliteCollectElem[A, I, CA])(implicit pos: SourceContext): Option[Exp[CA]] = {

    val alloc: Block[I] = t(body.buf.alloc)

    alloc match {

      case StructBlock(tag, elems) =>
        
        // Create new loop to produce DeliteArray[B] from f
        def copyLoop[B:Manifest](f: Block[B], c: Option[Exp[Boolean]]): Exp[DeliteArray[B]] = f match {
          case Block(Def(DeliteArrayApply(x,iv))) if iv.equals(v) && c == None =>
            x.asInstanceOf[Exp[DeliteArray[B]]] //eliminate identity function loop

          case _ =>   
            val elemV = fresh[B]
            val sizeV = fresh[Int]
            val allocV = reflectMutableSym(fresh[DeliteArray[B]])
            val tv = t(v).asInstanceOf[Sym[Int]]
            val elemF = fresh[DeliteCollection[B]]
            val indexF = fresh[Int]

            val simpleFunc = reifyEffects(DeliteArraySingletonInLoop(f, v))
            val newFunc = c.fold{ simpleFunc }{ cond => 
              reifyEffects(IfThenElse(cond, simpleFunc, reifyEffects(DeliteArrayEmptyInLoop(v, manifest[B]))))
            }

            val newBuf = getOutputStrategy(body) match {
              case OutputFlat => 
                DeliteCollectFlatOutput[B,DeliteArray[B],DeliteArray[B]](
                  eV = elemV,
                  sV = sizeV,
                  allocVal = allocV,
                  alloc = reifyEffects(DeliteArray[B](sizeV)),
                  update =  reifyEffects(dc_update(allocV, tv, elemV)),
                  finalizer = reifyEffects(allocV)
                )
              case OutputBuffer => 
                val allocV2 = fresh[DeliteArray[B]]
                val startIdx = fresh[Int]
                val endIdx = fresh[Int]

                DeliteCollectBufferOutput[B,DeliteArray[B],DeliteArray[B]](
                  eV = elemV,
                  sV = sizeV,
                  iV = startIdx,
                  iV2 = endIdx,
                  allocVal = allocV,
                  aV2 = allocV2,
                  alloc = reifyEffects(DeliteArray[B](sizeV)),
                  update =  reifyEffects(dc_update(allocV, tv, elemV)),
                  append = reifyEffects(dc_append(allocV, tv, elemV)),
                  appendable = reifyEffects(dc_appendable(allocV, tv, elemV)),
                  setSize = reifyEffects(dc_set_logical_size(allocV, sizeV)),
                  allocRaw = reifyEffects(dc_alloc[B,DeliteArray[B]](allocV, sizeV)),
                  copyRaw = reifyEffects(dc_copy(allocV2, startIdx, allocV, endIdx, sizeV)),
                  finalizer = reifyEffects(allocV)
                )
            }

            simpleLoop(t(size), tv, 
              DeliteCollectElem[B,DeliteArray[B],DeliteArray[B]](
                buf = newBuf,
                iFunc = newFunc,
                unknownOutputSize = body.unknownOutputSize,
                numDynamicChunks = body.numDynamicChunks,
                eF = elemF,
                iF = indexF,
                sF = reifyEffects(dc_size(elemF)),
                aF = reifyEffects(dc_apply(elemF, indexF))
              )
            )
        }
        

        def soaTransform[B:Manifest](tag: StructTag[B], elems: Seq[(String, Exp[Any])], cond: Option[Exp[Boolean]]): Exp[DeliteArray[B]] = {
          val newElems = elems map {
            case (index, e @ Def(Struct(t, es))) => (index, soaTransform(t, es, cond)(e.tp))
            case (index, e) => (index, copyLoop(Block(e), cond)(e.tp))
          }
          val sz = getOutputStrategy(body) match {
            case OutputFlat => t(size)
            //TODO: we want to know the output size without having to pick one of the returned 
            // arrays arbitrarily (prevents potential DCE)... 
            // can we just grab the size out of the activation record somehow?
            case OutputBuffer => newElems(0)._2.length 
          }

          struct[DeliteArray[B]](SoaTag(tag, sz), newElems)
        }

        val dataField: String = dc_data_field(t.getBlockResult(alloc).tp)
        val sizeField: String = dc_size_field(t.getBlockResult(alloc).tp)

        if (dataField == "" && !isSubtype(manifest[I].erasure,classOf[DeliteArray[_]])) {
          printlog("unable to transform collect elem: no data field defined for " + manifest[I].toString)
          return None
        }

        val (func: Block[A], cond) = getCollectElemType(t(body.iFunc)) match {
          case CollectAnyMap(elem, _) => 
            (elem, None)
          case CollectFilter(_, cond, elem, _, _) => 
            (elem, Some(cond))
          case CollectFlatMap => // TODO: can we change this ?
            printlog("unable to transform flatmap elem")
            return None
        }

        val newLoop = func match {
          case u@Block(Def(a@Struct(ta, es))) =>
            printlog("*** SOA " + u + " / " + a)
            soaTransform(ta, es, cond)
          case f2@Block(Def(a)) =>
            printlog("*** Unable to SOA " + f2 + " / " + a)
            copyLoop(f2, cond)
          case f2 => 
            copyLoop(f2, cond)
        }

        val res = if (isSubtype(manifest[I].erasure, classOf[DeliteArray[_]])) {
          newLoop
        } else {
          val newElems = elems.map {
            case (df, _) if df == dataField => (df, newLoop)
            case (sf, _) if (sf == sizeField) => getOutputStrategy(body) match {
                case OutputFlat => (sf, t(size))
                case OutputBuffer => (sf, newLoop.length)
              } 
            case (f, Def(Reflect(NewVar(init),_,_))) => t(init) match {
              case Def(x) if !(boundSyms(body) intersect syms(x)).isEmpty =>
                // This can happen if the field value was computed as a function of a bound symbol in the original loop (e.g. size)
                printlog("unable to unwrap struct: elem " + t(init) + " depends on a bound symbol in " + body)
                return None
              case _ => (f, t(init))
            }
            case (f,v) => t(v) match {
              case Def(x) if !(boundSyms(body) intersect syms(x)).isEmpty =>
                printlog("unable to unwrap struct: elem " + t(v) + " depends on a bound symbol in " + body)
                return None
              case _ => (f, t(v))
            }
          }

          struct[I](tag, newElems)
        }

        t.replace(body.buf.allocVal, res) // TODO: use withSubstScope
        printlog("successfully transformed collect elem with type " + manifest[I].toString + " to " + res.toString)
        Some(t.getBlockResult(t(body.buf.finalizer)))
        
              
      case Block(Def(Reify(s@Def(a),_,_))) => 
        printlog("unable to transform collect elem: found " + s.toString + ": " + a + " with type " + manifest[I].toString)
        None
    
      case a => 
        printlog("unable to transform collect elem: found " + a + " with type " + manifest[I].toString)
        None
    }
  }


  //hash collect elems: similar to collect elems; we only transform the values, not the keys
  //TODO


  //reduce elems: unwrap result if elem is a Struct
//  def soaReduce[A:Manifest](size: Exp[Int], v: Sym[Int], body: DeliteReduceElem[A]): Option[Exp[A]] = t(body.func) match {
//    case StructBlock(tag,elems) =>
//      def copyLoop[B:Manifest](f: Block[B], r: Block[B], z: Block[B], rv1: Exp[B], rv2: Exp[B]): Exp[B] = {
//        simpleLoop(t(size), t(v).asInstanceOf[Sym[Int]], DeliteReduceElem[B](
//          func = f,
//          cond = body.cond.map(t(_)),
//          zero = z,
//          accInit = reifyEffects(fatal(unit("accInit not transformed")))(manifest[B]), //unwrap this as well to support mutable reduce
//          rV = (rv1.asInstanceOf[Sym[B]], rv2.asInstanceOf[Sym[B]]),
//          rFunc = r,
//          stripFirst = !isPrimitiveType(manifest[B]),
//          numDynamicChunks = body.numDynamicChunks
//        ))
//      }
//
//      def soaTransform[B:Manifest](func: Block[B], rFunc: Block[B], zero: Block[B], rv1: Exp[B], rv2: Exp[B]): Exp[B] = func match {
//        case Block(f @ Def(Struct(_,_))) => rFunc match {
//          case Block(r @ Def(Struct(tag,elems))) => zero match { //TODO: mutable reduce? reflectMutableSym on rV causes issues...
//            case Block(z @ Def(Struct(_,_))) =>
//              val ctx = implicitly[SourceContext]
//              val newElems = elems map {
//                case (i, e @ Def(Struct(_,_))) => (i, soaTransform(Block(field(f,i)(e.tp,ctx)), Block(field(r,i)(e.tp,ctx)), Block(field(z,i)(e.tp,ctx)), field(rv1,i)(e.tp,ctx), field(rv2,i)(e.tp,ctx))(e.tp))
//                case (i, c@Const(_)) => (i, c)
//                case (i, e) => (i, copyLoop(Block(field(f,i)(e.tp,ctx)), Block(field(r,i)(e.tp,ctx)), Block(field(z,i)(e.tp,ctx)), field(rv1,i)(e.tp,ctx), field(rv2,i)(e.tp,ctx))(e.tp))
//              }
//              struct[B](tag, newElems)
//            case Block(Def(a)) =>
//              Console.println(a)
//              sys.error("transforming reduce elem but zero is not a struct and func is")
//          }
//          case Block(Def(a)) =>
//            Console.println(a)
//            sys.error("transforming reduce elem but rFunc is not a struct and func is")
//        }
//        case Block(Def(a)) =>
//          Console.println(a)
//          sys.error("transforming reduce elem but func is not a struct and rFunc is")
//      }
//
//      val (rv1, rv2) = unwrapRV(tag, elems, body.rV)
//      val res = soaTransform(t(body.func), t(body.rFunc), t(body.zero), rv1, rv2)
//      printlog("successfully transformed reduce elem with type " + manifest[A])
//      Some(res)
//
//    case a => printlog("unable to transform reduce elem: found " + a + " with type " + manifest[A]); None
//  }

  private def unwrapRV[B:Manifest](tag: StructTag[B], elems: Seq[(String,Exp[Any])], rv: (Exp[B], Exp[B])): (Exp[B], Exp[B]) = {
    def makeRV[B:Manifest](tag: StructTag[B], elems: Seq[(String,Exp[Any])]): Exp[B] = {
      val newElems = elems.map {
        case (index, e @ Def(Struct(t,es))) => (index, makeRV(t,es)(e.tp))
        case (index, e) => (index, fresh(e.tp))
      }
      struct[B](tag, newElems)
    }

    val new_rv1 = makeRV(tag,elems)
    val new_rv2 = makeRV(tag,elems)
    t.replace(rv._1, new_rv1)
    t.replace(rv._2, new_rv2)
    (new_rv1, new_rv2)
  }


  //hash reduce elems: similar to reduce elems; we only transform the values, not the keys
  def soaHashReduce[K:Manifest,V:Manifest,I:Manifest,CV:Manifest](size: Exp[Int], v: Sym[Int], body: DeliteHashReduceElem[K,V,I,CV]): Option[Exp[CV]] = {
    val alloc = t(body.buf.alloc)
    alloc match {
    case StructBlock(tag,elems) =>
      val condT = body.cond.map(t(_))
      val keyT = t(body.keyFunc)
      val valT = t(body.valFunc)
      val tv = t(v).asInstanceOf[Sym[Int]]
      val sizeT = t(size)

      def copyLoop[B:Manifest](f: Block[B], r: Block[B], z: Block[B], rv1: Exp[B], rv2: Exp[B]): Exp[DeliteArray[B]] = {
        val allocV = reflectMutableSym(fresh[DeliteArray[B]])
        val indexV = fresh[Int]
        val sizeV = fresh[Int]
        val elemV = fresh[B]
        simpleLoop(sizeT, tv, DeliteHashReduceElem[K,B,DeliteArray[B],DeliteArray[B]](
          keyFunc = keyT,
          valFunc = f,
          cond = condT,
          zero = z,
          rV = (rv1.asInstanceOf[Sym[B]], rv2.asInstanceOf[Sym[B]]),
          rFunc = r,
          buf = DeliteBufferElem[B,DeliteArray[B],DeliteArray[B]](
            eV = elemV,
            sV = sizeV,
            iV = indexV,
            iV2 = unusedSym,
            allocVal = allocV,
            aV2 = unusedSym,
            alloc = reifyEffects(DeliteArray[B](sizeV)),
            apply = reifyEffects(dc_apply(allocV,indexV)),
            update = reifyEffects(dc_update(allocV,indexV,elemV)),
            appendable = unusedBlock,
            append = reifyEffects(dc_append(allocV,tv,elemV)),
            setSize = reifyEffects(dc_set_logical_size(allocV,sizeV)),
            allocRaw = unusedBlock,
            copyRaw = unusedBlock,
            finalizer = reifyEffects(allocV)
          ),
          numDynamicChunks = body.numDynamicChunks
        ))
      }

      def soaTransform[B:Manifest](func: Block[B], rFunc: Block[B], zero: Block[B], rv1:Exp[B], rv2: Exp[B]): Exp[DeliteArray[B]] = func match {
        case Block(f @ Def(vf@Struct(_,_))) => rFunc match {
          case Block(r @ Def(Struct(tag,elems))) => zero match {
            case Block(z @ Def(Struct(_,_))) =>
              val ctx = implicitly[SourceContext]
              val newElems = elems map {
                case (i, e @ Def(Struct(_,_))) => (i, soaTransform(Block(field(f,i)(e.tp,ctx)), Block(field(r,i)(e.tp,ctx)), Block(field(z,i)(e.tp,ctx)), field(rv1,i)(e.tp,ctx), field(rv2,i)(e.tp,ctx))(e.tp))
                case (i, e) => (i, copyLoop(Block(field(f,i)(e.tp,ctx)), Block(field(r,i)(e.tp,ctx)), Block(field(z,i)(e.tp,ctx)), field(rv1,i)(e.tp,ctx), field(rv2,i)(e.tp,ctx))(e.tp))
              }
              struct[DeliteArray[B]](SoaTag(tag, newElems(0)._2.length), newElems) //TODO: output size
            case Block(s@Def(a)) =>
              Console.println(f.toString + ": " + vf.toString + " with type " + f.tp.toString)
              Console.println(s.toString + ": " + a.toString + " with type " + s.tp.toString)
              sys.error("transforming hashReduce elem but zero is not a struct and valFunc is")
          }
          case Block(Def(a)) =>
            Console.println(a)
            sys.error("transforming hashReduce elem but rFunc is not a struct and valFunc is")
        }
        case Block(Def(a)) =>
          Console.println(a)
          sys.error("transforming hashReduce elem but valFunc is not a struct and rFunc is")
      }

      val dataField = dc_data_field(t.getBlockResult(alloc).tp) //FIXME
      val sizeField = dc_size_field(t.getBlockResult(alloc).tp)

      if (dataField == "" && !isSubtype(manifest[I].erasure,classOf[DeliteArray[_]])) {
        printlog("unable to transform hashReduce elem: no data field defined for " + manifest[I].toString)
        return None
      }

      val newLoop = valT match {
        case Block(Def(Struct(ta,es))) =>
          val (rv1, rv2) = unwrapRV(ta,es,body.rV)
          soaTransform(valT, t(body.rFunc), t(body.zero), rv1, rv2)
        case _ => copyLoop(valT, t(body.rFunc), t(body.zero), t(body.rV._1), t(body.rV._2))
      }

      val res = if (isSubtype(manifest[I].erasure, classOf[DeliteArray[_]])) {
        newLoop
      }
      else {
        val newElems = elems.map {
          case (df, _) if df == dataField => (df, newLoop)
          case (sf, _) if sf == sizeField => (sf, newLoop.length)
          case (f, Def(Reflect(NewVar(init),_,_))) => t(init) match {
            case Def(x) if !(boundSyms(body) intersect syms(x)).isEmpty =>
              printlog("unable to unwrap struct: elem " + t(init) + " depends on a bound symbol in " + body)
              return None
            case _ => (f, t(init))
          }
          case (f,v) => t(v) match {
            case Def(x) if !(boundSyms(body) intersect syms(x)).isEmpty =>
              printlog("unable to unwrap struct: elem " + t(v) + " depends on a bound symbol in " + body)
              return None
            case _ => (f, t(v))
          }
        }
        struct[I](tag, newElems)
      }

      t.replace(body.buf.allocVal, res) // TODO: use withSubstScope
      printlog("successfully transformed hashReduce elem with type " + manifest[I].toString + " to " + res.toString)
      Some(t.getBlockResult(t(body.buf.finalizer)))

    case Block(Def(Reify(Def(a),_,_))) => printlog("unable to transform hashReduce elem: found " + a + " with type " + manifest[CV].toString); None
    case _ => None
  } }

  //case class FieldFromEffect[T](field: Exp[T], effects: Block[Unit]) extends Def[T]
  //val block = Block(Reify(Const(()), es, u))
  //struct(tag, elems.map(e => (e._1, effectField(e._2, block))))

  def unwrapSingleTask[A:Manifest](s: DeliteOpSingleTask[A]): Option[Exp[A]] = s.block match {
    case Block(Def(e@Struct(_,_))) => Console.println("unwrapped sequential: " + e.toString); Some(e)
    //case Block(Def(Reify(Def(Struct(_,_)),es,u))) => Console.println("unwrapped sequential with reify: " + e.toString)
    case Block(Def(a)) => Console.println("failed on sequential: " + a.toString); None
  }

}

trait LoopSoAOpt extends BaseGenLoopsFat with LoopFusionOpt {
  val IR: DeliteOpsExp
  import IR._

  //pre-fuse any loops that have been split by SoA transform
  //such loops have the same 'size' sym, the same 'v' sym, and are in the same scope
  override def focusExactScopeFat[A](resultB: List[Block[Any]])(body: Seq[Stm] => A): A = {
    if (Config.soaEnabled) {
      val result = resultB.map(getBlockResultFull) flatMap { case Combine(xs) => xs case x => List(x) }
      val currentScope = innerScope
      val levelScope = getExactScope(currentScope)(result) //top-level scope

      val loops = levelScope collect { case t@TTP(_, _, SimpleFatLoop(_,_,_)) => t }
      val splitLoops = loops groupBy { case TTP(_, _, SimpleFatLoop(size,v,bodies)) => v }

      val fusedLoops = splitLoops map {
        case (v, l) if l.length == 1 => l.head
        case (v, s) =>
          val l = s.toList
          val size = l.map(_.rhs.asInstanceOf[AbstractFatLoop].size) reduceLeft { (s1,s2) => assert(s1 == s2); s1 }
          val t = TTP(l.flatMap(_.lhs), l.flatMap(_.mhs), SimpleFatLoop(size, v, l.flatMap(_.rhs.asInstanceOf[AbstractFatLoop].body)))
          printlog("fusing split SoA loop: " + t.toString)
          t
      }

      val remainder = currentScope diff loops
      val newScope = remainder ++ fusedLoops
      innerScope = getSchedule(newScope)(result) //get the order right
    }
    super.focusExactScopeFat(resultB)(body) //call LoopFusionOpt
  }

}

package ppl.apps.assignment2

import ppl.dsl.assignment2.{SimpleVectorApplicationRunner, SimpleVectorApplication}

object SimpleVectorAppRunner extends SimpleVectorApplicationRunner with SimpleVectorApp

trait SimpleVectorApp extends SimpleVectorApplication {

  def main() {
    val x = Vector[Int](5) + 1
    val y = Vector[Int](5) + 2

    val z = x + y
    z.pprint

    val f = z.filter(_ % 2 == 1)
    f.pprint
    
    val res = z.sum
    println(res)
  }
}

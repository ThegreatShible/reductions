package reductions

import org.scalameter._
import common._

import scala.annotation.tailrec

object LineOfSightRunner {
  
  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 100,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]) {
    val length = 10000000
    val input = (0 until length).map(_ % 100 * 1.0f).toArray
    val output = new Array[Float](length + 1)
    val seqtime = standardConfig measure {
      LineOfSight.lineOfSight(input, output)
    }
    println(s"sequential time: $seqtime ms")

    val partime = standardConfig measure {
      LineOfSight.parLineOfSight(input, output, 10000)
    }
    println(s"parallel time: $partime ms")
    println(s"speedup: ${seqtime / partime}")
  }
}

object LineOfSight {

  def max(a: Float, b: Float): Float = if (a > b) a else b

  def lineOfSight(input: Array[Float], output: Array[Float]): Unit = {
    val length = input.length
    def line(idx : Int = 1, maxi  : Float = 0): Unit  = {
      if(idx < length) {
        val value = input(idx) /  idx
        val maximum  = max( value , maxi)
        /*if(maximum  == value) {
          output(idx2) = value
          line(idx +1 , idx2 +1 , maximum)
        }else line(idx +1 , idx2 , maximum)*/

        output(idx) = maximum
        line(idx +1 , maximum)
      }
    }

    line()
    input(0) = 0
    output(0) = 0
  }

  sealed abstract class Tree {
    def maxPrevious: Float
  }

  case class Node(left: Tree, right: Tree) extends Tree {
    val maxPrevious = max(left.maxPrevious, right.maxPrevious)
  }

  case class Leaf(from: Int, until: Int, maxPrevious: Float) extends Tree

  /** Traverses the specified part of the array and returns the maximum angle.
   */
  def upsweepSequential(input: Array[Float], from: Int, until: Int): Float = {
   /*  if(from - until == 1) input(from)
    else max(input(from) , upsweepSequential(input , from +1  , until ))*/
    var i = from
    var maxi = input(from)/from
    while(i < until){
      maxi = max( maxi , input(i)/i)
      i+=1
    }
    maxi
  }

  /** Traverses the part of the array starting at `from` and until `end`, and
   *  returns the reduction tree for that part of the array.
   *
   *  The reduction tree is a `Leaf` if the length of the specified part of the
   *  array is smaller or equal to `threshold`, and a `Node` otherwise.
   *  If the specified part of the array is longer than `threshold`, then the
   *  work is divided and done recursively in parallel.
   */
  def upsweep(input: Array[Float], from: Int, end: Int,
    threshold: Int): Tree = {
    if(end - from  <=  threshold) Leaf(from , end , upsweepSequential(input , from , end))
    else {
      val mid = (from + end) / 2
      val (left , right) = parallel(upsweep(input ,from , mid  , threshold) , upsweep(input , mid , end , threshold))
      Node(left , right)
    }
  }

  /** Traverses the part of the `input` array starting at `from` and until
   *  `until`, and computes the maximum angle for each entry of the output array,
   *  given the `startingAngle`.
   */
 @tailrec def downsweepSequential(input: Array[Float], output: Array[Float],
    startingAngle: Float, from: Int, until: Int): Unit = {
    if(from < until){
      val maximum = if(from == 0 ) 0f else max(startingAngle , input(from)/from)
      output(from) = maximum
      downsweepSequential(input , output , maximum , from +1  , until)
    }

  }

  /** Pushes the maximum angle in the prefix of the array to each leaf of the
   *  reduction `tree` in parallel, and then calls `downsweepTraverse` to write
   *  the `output` angles.
   */
  def downsweep(input: Array[Float], output: Array[Float], startingAngle: Float,
    tree: Tree): Unit = tree match {
    case tree : Leaf =>  downsweepSequential(input , output, startingAngle , tree.from , tree.until)
    case tree : Node =>  parallel(downsweep(input , output, startingAngle, tree.left) , downsweep(input , output ,max(startingAngle,tree.left.maxPrevious) , tree.right))

  }

  /** Compute the line-of-sight in parallel. */
  def parLineOfSight(input: Array[Float], output: Array[Float],
    threshold: Int): Unit = {
    val tree = upsweep(input , 0 , input.length , threshold)
    downsweep(input , output , 0 , tree)
  }
}

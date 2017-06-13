package ja.com

import ja.conf.JobSparkConf

/**
  * Created by Ja on 07/06/2017.
  */
object groupbyex {

  def main(args: Array[String]): Unit = {

    // Bazic groupBy example in scala
    val x = JobSparkConf.sc.parallelize(Array("Joseph", "Jimmy", "Tina", "Thomas", "James", "Cory", "Christine", "Jackeline", "Juan"), 3)
  //   println(x.toString())prin
    println(x.toString())
    println(x)

  }
}

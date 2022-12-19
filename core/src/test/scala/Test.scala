

object Test {
  def main(args: Array[String]): Unit = {
    val arr: Array[Int] = Array(1, 3, 5, 7)
    // scalastyle:off
    println(arr.mkString("Array(", ", ", ")"))

    arr(3) -= 4

    println(arr(3))

    println(arr.mkString("Array(", ", ", ")"))
    // scalastyle:on

  }
}

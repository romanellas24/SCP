package project.utils

import java.io.PrintWriter

class Printer {
  def saveOutput(list: List[(Int, Int, Int)], path: String): Unit = {
    new PrintWriter(path) {
      list.foreach(e => {
        val (x, y, z) = e
        write(x + "," + y + "," + z + "\n")
      })
      close()
    }
  }
}

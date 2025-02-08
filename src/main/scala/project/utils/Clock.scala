package project.utils

class Clock (val start: Long = System.currentTimeMillis()) {

  def getElapsedTime(): Long = {
    System.currentTimeMillis() - this.start
  }

  def printElapsedTime(): Unit = {
    println("Execution time: " + this.getElapsedTime() + "ms")
  }

}

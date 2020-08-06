package covidimpact

import java.util.logging.Logger

object Util {
  def time[T](block: => T): T = {
    val t0 = System.nanoTime()
    println("")
    Logger.getLogger("Report").info("Starting timer...")
    val res = block
    val t1 = System.nanoTime()
    Logger.getLogger("Report").info(s"Time elapsed: ${(t1 - t0)/1e6}ms")
    res
  }

}

val a = Seq(1,2,3,4)
val b = a.tail

val z = a zip b
z map (a => a._1 + a._2)

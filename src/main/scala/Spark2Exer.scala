

object Spark2Exer extends App {
  val sparkSession = Spark.createLocalSession

  //Lectura de Quijote y M&M
  val quijote=sparkSession.read.text("src/main/resources/spark2/el_quijote.txt")
  val mm = sparkSession.read.option("header", true).csv("src/main/resources/spark2/mnm_dataset.csv")

  /*
  quijote.show(2,false)
  println(quijote.count())
  println(quijote.head())
  println(quijote.take(2))
  println(quijote.first())
*/

}

import org.apache.spark.sql.functions.{avg, col, max, min, to_date, when, year}

object Spark2ExerC2 extends App {

  val sparkSession = Spark.createLocalSession

  //Lectura de Quijote y M&M
  val quijote=sparkSession.read.text("src/main/resources/spark2/el_quijote.txt")
  val mm = sparkSession.read.option("header", true).csv("src/main/resources/spark2/mnm_dataset.csv")
  // Mostrar el contenido del quijote y probar first, head y take

  /*
  quijote.show(2,false)
  println(quijote.count())
  println(quijote.head())
  println(quijote.take(2))
  println(quijote.first())
  */

  //Hacer una query con ordenación descendente
  val maxmm = mm.groupBy("State").agg(avg("Count").as("Media")).orderBy(col("Media").desc)
  //maxmm.show()

  // Mostrar solo los estados CA, CO y TX
  val statesmm = mm.where(col("State").contains("CA") or col("State").contains("CO") or col("State").contains("TX"))
  //statesmm.show()

  // Dar media, max y min por color
  val avgmm = mm.groupBy("Color").agg(avg("Count"), max("Count"), min("Count"))
  //avgmm.show

  //hacer una consulta usando sql (temp view)
  mm.createTempView("mandm")
  val mmSql = sparkSession.sql("SELECT DISTINCT(Color),State FROM mandm WHERE State LIKE 'C%' ORDER BY Color")
  //mmSql.show

}

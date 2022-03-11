import org.apache.commons.collections.ComparatorUtils
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{avg, col, max, min, to_date, when, year}
import org.apache.spark.sql.types.TimestampType

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
  println(quijote.count())
  */
  val maxmm = mm.agg(functions.max(col("Color")))
  //maxmm.show()
  val statesmm = mm.where(col("State").contains("CA") or col("State").contains("CO") or col("State").contains("TX"))
  //statesmm.show()
  val avgmm = mm.groupBy("Color").agg(avg("Count"), max("Count"), min("Count"))
  //avgmm.show
  //hacer una consulta usando sql (temp view)
  mm.createTempView("mandm")
  val mmSql = sparkSession.sql("SELECT DISTINCT(Color),State FROM mandm WHERE State LIKE 'C%' ORDER BY Color")
  //mmSql.show
  //cap3
  //Lectura firecall
  val firec = sparkSession.read.option("header","true").option("inferSchema","true").csv("src/main/resources/spark2/sf-fire-calls.csv")
  //Ver esqema por defecto
  //firec.printSchema()
  // Cambiar String a formato fecha y darle algún filtro
  val fireDate = firec.select(to_date(col("CallDate"),"MM/dd/yy").as("Call_date"),col("NumAlarms"),col("Location"))
    .filter(year(col("Call_date")).equalTo(2015))
  //fireDate.show()
  //Guargar de varias formas
  //json
 // fireDate.coalesce(1).write.json("dataset/cap3/fire")
  //csv
 // fireDate.coalesce(1).write.csv("dataset/cap3/fire1")
  //avro
 // fireDate.coalesce(1).write.format("avro").save("dataset/cap3/fire2")

  //cap4
  val flight = sparkSession.read.option("header",true).csv("src/main/resources/spark2/departuredelays.csv")

  //flight.printSchema()
  //flight.show
  flight.createTempView("flightdf")
  //sparkSession.sql("SELECT origin, destination FROM flightdf WHERE origin == 'ORD'").show()
  val query2 = flight.withColumn("demoras",
    when(col("delay")> 360,"Demora muy larga")
      .when(col("delay")> 120,"Demora larga")
      .when(col("delay")> 60,"Demora corta")
      .when(col("delay")> 0,"Demora normal")
      .otherwise("Sin demoras")).select("delay","origin","destination","demoras")
    .orderBy(col("origin"))
  query2.show()

  //Lectura de los archivos creados
 // val firedf = sparkSession.read.json("dataset/cap3/fire")
 // val firedf1 = sparkSession.read.csv("dataset/cap3/fire1")
//  val firedf2 = sparkSession.read.format("avro").load("dataset/cap3/fire2")
  //cap5
//val empleados = sparkSession.read

}

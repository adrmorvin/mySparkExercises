import org.apache.spark.sql.functions
import org.apache.spark.sql.functions._

object Spark2ExerC4 extends App {
  val sparkSession = Spark.createLocalSession

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


}

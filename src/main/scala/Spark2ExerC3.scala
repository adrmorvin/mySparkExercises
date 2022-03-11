import org.apache.spark.sql.functions._

object Spark2ExerC3 extends App {

  val sparkSession = Spark.createLocalSession

  //Lectura firecall
  val firec = sparkSession.read.option("header","true").option("inferSchema","true")
    .csv("src/main/resources/spark2/sf-fire-calls.csv")

  //Ver esqema por defecto
  //firec.printSchema()

  // Cambiar String a formato fecha y darle algún filtro
  val fireDate = firec.select(to_date(col("CallDate"),"MM/dd/yy").as("Call_date"),col("NumAlarms"),col("Location"))
    .filter(year(col("Call_date")).equalTo(2015)).orderBy(col("NumAlarms").desc)
  //fireDate.show(false)

  // Otro filtro aprovechando la fecha
  val fireData1 = firec.select(to_date(col("CallDate"),"MM/dd/yy").as("Call_date"),col("City"),col("Delay"))
    .filter(col("Delay").lt(24) and col("City").isNotNull and month(col("Call_date"))
      .equalTo(4)).orderBy("Call_date")
  //fireData1.show(false)

  //Guargar de varias formas

  //json
  // fireDate.coalesce(1).write.json("dataset/cap3/fire")

  // csv
  // fireDate.coalesce(1).write.csv("dataset/cap3/fire1")

  //avro
  // fireDate.coalesce(1).write.format("avro").save("dataset/cap3/fire2")


}

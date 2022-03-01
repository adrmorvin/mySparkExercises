import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions.{col, date_format, from_unixtime, month, to_date, year}
import org.apache.spark.sql.types.TimestampType

object exer5 extends App {

  implicit val sparkSession = Spark.createLocalSession
/*
* 1. Leer los datos desde orders-parquet
* 2. Mostrar solo aquellas filas en lsa que el estado sea COMPLETE
* 3. Fecha en formato dd/MM/yyyy
* 4. Solo mostrar datos de 2014 en Enero o Julio
* 5. Guardar en json
* */
  //Si no tuviera cabecera o quiseramos cambiar algun tipo como la fecha sería así
/*
  val myschema= StructType(Array(
    StructField("id", StringType,true),
    StructField("order_date", LongType,true),
    StructField("customer_id", IntegerType,true),
    StructField("order_status", StringType,true)))
*/
  implicit val ordersDF: DataFrame = MainRead.readParquet("src/main/resources/retail_db/orders_parquet/741ca897-c70e-4633-b352-5dc3414c5680.parquet")
// Comprobamos que los datos esten bien
 // ordersDF.show()

  val ordDF = ordersDF.filter(col("order_status").equalTo("COMPLETE")).select(col("order_date"),col("order_id"),
      to_date(from_unixtime(col("order_date")/1000,"yyyy-MM-dd HH:mm:ss"),"yyyy-MM-dd HH:mm:ss").as("order_date1"),
      col("order_status"))
        .withColumn( "newDate",to_date((col("order_date")/1000).cast(TimestampType)))
  ordDF.show()
   val sol5 = ordDF.filter(year(col("order_date1")).equalTo(2014) and
      (month(col("newDate")).equalTo(1) or month(col("newDate")).equalTo(7)))
    .select(col("order_id"),
      date_format(col("newDate"),"dd-MM-yyyy").as("newDate"),
      col("order_status"), col("order_date"))


//Vemos que nos sale
  sol5.show()

//Lo guardamos
  sol5.write.mode(SaveMode.Overwrite).json("dataset/q5/solution/porfecha.json")

}

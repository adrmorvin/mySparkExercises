import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions.{col, date_format, from_unixtime, to_date, year}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

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
 val myschema= StructType(Array(
    StructField("id", IntegerType,true),
    StructField("order_date", StringType,true),
    StructField("customer_id", IntegerType,true),
    StructField("status", StringType,true)))

  implicit val ordersDF: DataFrame = MainRead.readParquet("src/main/resources/retail_db/orders_parquet/741ca897-c70e-4633-b352-5dc3414c5680.parquet",myschema)
// Comprobamos que los datos esten bien
ordersDF.show()
//realizamos todos los filtros
  val sol5 = ordersDF.select(ordersDF.col("order_id"),ordersDF.col("order_status"),to_date(ordersDF.col("order_date"), "dd/MM/yyyy")).as("dateM")
    .where("order_status =='COMPLETE'").filter("year(dateM) == 2014 and month(dateM) == (01 or 07)")
//Vemos que nos sale
  sol5.show()

//Lo guardamos

  //sol5.write.mode(SaveMode.Overwrite).json("dataset/q5/solution/porfecha.json")

}

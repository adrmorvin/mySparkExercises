import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SaveMode}

object exer1 extends App{

  /*
 * 1. Leer el contenido de products_avro
 * 2. Filtrar dicho contenido para que el precio de los productos sea mayor o
 * igual que 20 y menor o igual que 23
 * 3. Quedarnos solo con los productos que comiencen por Nike
 * 4. Se espera que se use gZip compression
  */
  implicit val sparkSession = Spark.createLocalSession
  val productDF: DataFrame = MainRead.readAvro("src/main/resources/retail_db/products_avro/")
  //Una vez que ya tenemos los datos observamos el schema
  productDF.printSchema()
  //Realizamos los filtros correspondientes (2-3)
  val sol1 = productDF.filter(col("product_name").startsWith("Nike") and col("product_price").leq("23") and col("product_price").geq("20"))
  //Mostramos el resultado obtenido
  //sol1.show()
  //Guardamos el fichero con la compresion gZip
  sol1.write.mode(SaveMode.Overwrite).option("codec", "org.apache.hadoop.io.compress.GzipCodec").csv("dataset/q1/solution")

}

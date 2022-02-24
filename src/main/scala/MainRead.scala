import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
//Aqui se muestran  lecturas de los datos para ser usada cuando se necesiten
object MainRead {
  def readCsvHeader(ruta:String)(implicit sparkSession: SparkSession)={
    sparkSession.read.option("header","true").csv(ruta)
  }
  def readCsvSchema(ruta:String, myschema: StructType)(implicit sparkSession: SparkSession)= {
    sparkSession.read.schema(myschema).csv(ruta)
  }
  def readCsvSchemaSpace(ruta:String, myschema: StructType)(implicit sparkSession: SparkSession)={
    sparkSession.read.schema(myschema).option("sep","\t").csv(ruta)
  }
  def readAvro(ruta:String)(implicit sparkSession: SparkSession)={
    sparkSession.read.format("com.databricks.spark.avro").load(ruta)
  }
  def readParquet(ruta:String, myschema: StructType)(implicit sparkSession: SparkSession)= {
    sparkSession.read.schema(myschema).option("header","true").load(ruta)
  }
  def suma (x:Int,y:Int):Int ={
    x+y
  }
  def resta (x:Int,y:Int):Int = {
    x-y
  }

}

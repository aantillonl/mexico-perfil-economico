import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.parquet.format.IntType
import org.apache.spark.sql.types.{StructType, StructField, IntegerType, DoubleType, StringType}

object EconomicProfile {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Mexico Economic Profile")
      .getOrCreate()

    import spark.implicits._

    // Load raw economic census data (sample file for now)
    val inputPath = "data/conjunto_de_datos/*.csv"

    // Select columns of interest: geography, sector and value variables
    val idCols = Seq("ENTIDAD", "UE")
    val valueCols = Seq(
      "A111A", // Producción bruta total
      "A121A", // Consumo intermedio
      "A131A", // Valor agregado censal bruto
      "A211A", // Inversión total
      "A221A", // Formación bruta de capital fijo
      "A511A", // Margen por reventa
      "J000A", // Remuneraciones totales
      "H001A", // Personal ocupado total
      "K000A"  // Gastos por consumo
    )

    val rawDf = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .option("ignoreLeadingWhiteSpace", "true")
      .csv(inputPath)
      .select(
        idCols.map(col(_).cast(IntegerType))++
        valueCols.map(col(_).cast(DoubleType))
        : _*
      )

    val activityColsSchema = StructType(
      Array(
        StructField("CODIGO", IntegerType, true),
        StructField("DESC_CODIGO", StringType, true),
        StructField("CLASIFICADOR_CODIGO", StringType, true)
      )
    )
  
    val activityCodesPath = "data/catalogos/tc_codigo_actividad.csv"
    val activitiesDf = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .schema(activityColsSchema)
      .csv(activityCodesPath)

    val sectorsDF = activitiesDf.where(col("CLASIFICADOR_CODIGO").equalTo("Sector"))

    val activitiesBySector = rawDf
      .where(rawDf("ENTIDAD").isNotNull)
      .join(sectorsDF, rawDf("UE").equalTo(sectorsDF("CODIGO"))) 

    // Group by ENTIDAD and SECTOR, aggregate economic indicators
    val aggExprs = valueCols.map(vc => sum(vc).alias(s"sum_$vc"))
    val summaryDf = activitiesBySector
      .groupBy("ENTIDAD", "CODIGO")
      .agg(aggExprs.head, aggExprs.tail:_*)

    // Output path for Parquet summary
    val outputPath = "data/output/state_sector_summary"
    summaryDf.write.mode("overwrite").parquet(outputPath)

    // Select only the top 5 activities per state
    val w = Window.partitionBy("ENTIDAD").orderBy(col("sum_A131A").desc)
    val topActivitiesByState = summaryDf
      .withColumn(
        "activity_rank",
        rank().over(w),
        )
      .filter("activity_rank <= 5")
      .select("ENTIDAD", "activity_rank")

    topActivitiesByState.write.mode("overwrite")
      .parquet("data/output/top_activities_by_state")
    
    // Calculate the percentage of the top activity to a state's total economic activity
    // This metric indicates dominance or over reliance on a single activity
    val topActivitShareDf = summaryDf
      .groupBy("ENTIDAD")
      .agg((max("sum_A131A")/sum("sum_A131A")).alias("hhi"))
      
    topActivitShareDf.write.mode("overwrite")
      .parquet("data/output/top_activity_share")

    spark.stop()
  }
}
package MyDataProducts.raw

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions.from_json

/**
 * This object provides raw access to data stored on disk.
 * This pattern could also wrap any source of data including Kafka or RDBMS as long as it returns a DataFrame.
 *
 * @example <pre>
 * z.load("/path/to.jar")
 * import MyDataProducts.raw.ProductRawStock
 * val spark = SparkSession.builder.getOrCreate()
 * import spark.implicits._
 * val df = spark.productRawStock()
 * </pre>
 */
object ProductRawStock {

  /**
   * This is the main container for each stock
   * @param stock_code ASX Stock symbol
   * @param company_name Company name for the stock symbol
   * @param price Current trading price
   * @param change The amount in $ that the price has changed +/-
   * @param volume Volume of stock been traded
   */
  case class StockData(
                        stock_code: String = "EMPTY",
                        company_name: String,
                        price: Float,
                        change: Float,
                        volume: Float
                      )

  /**
   * This is the main transformation access logic and typically contains minimal transformation
   * @param sparkSession Apache Spark session to use
   * @return Dataset in a hierarchy of OutputData
   */
  def transform(sparkSession: SparkSession): Dataset[StockData] = {
    import sparkSession.implicits._

    // This is configuration contains the location of the source data
    val inputPath = "/data/parquet"

    // Simpler to use reflection on the case class then duplicate the schema definition as Spark Structs
    val rawSchema = ScalaReflection.schemaFor[StockData].dataType.asInstanceOf[StructType]

    // Use Apache Spark to read the data (lazily) and return a Dataset with named elements
    sparkSession.read
      .format("parquet")
      .load(inputPath)
      // @example "{ \"company_name\": \"Abilene Oil & Gas\"    , \"price\": 0.002, \"change\": 0.00, \"volume\": 1000 }"
      .withColumn("raw", from_json($"details", rawSchema))
      .select("stock_code", "raw.company_name", "raw.price", "raw.change", "raw.volume")
      .as[StockData]
  }

  implicit class SparkSessionWithProductRawMenus(sparkSession: SparkSession) {
    def productRawStock(): Dataset[StockData] = {
      transform(sparkSession)
    }
  }

}
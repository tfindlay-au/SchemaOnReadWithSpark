package MyDataProducts.features

import MyDataProducts.raw.ProductRawStock
import MyDataProducts.raw.ProductRawStock.StockData
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.col

/**
 * This object provides a defined feature for a use case.
 * It would typically involved leveraging a `raw` product and applying enrichment's and filters often returning a
 * different structure specifically for a given use case
 *
 * @example <pre>
 *          z.load("/path/to.jar")
 *          import MyDataProducts.features.ProductTrendingStocks
 *          val spark = SparkSession.builder.getOrCreate()
 *          import spark.implicits._
 *          val df = spark.productTrendingStocks()
 *          </pre>
 */
object ProductTrendingStocks {

  /**
   * The transformation logic is applied here...
   * @param sparkSession Spark instance to used
   * @return Dataset of StockData or you could define a new structure in this object
   */
  def transform(sparkSession: SparkSession): Dataset[StockData] = {
    import MyDataProducts.enriched.PriceCalculations._
    import sparkSession.implicits._

    // Leverage the logic to acquire the raw data
    ProductRawStock.transform(sparkSession)
      .toDF()
      .withTrending()
      .where(col("is_trending_up"))
      .drop("is_trending_up")
      .as[StockData]
  }

  implicit class SparkSessionWithProductVegetarianMenus(sparkSession: SparkSession) {
    def productTrendingStocks(): Dataset[StockData] = {
      transform(sparkSession)
    }
  }

}

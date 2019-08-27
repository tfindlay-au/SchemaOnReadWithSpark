package MyDataProducts.enriched

import MyDataProducts.raw.ProductRawStock.StockData
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.sources.GreaterThan

/**
 * This object provides common logic used to enrich some data
 *
 * These are often used in the production of `features` or can be used by consumers if/when needed.
 */
object PriceCalculations {

  /**
   * This method performs a simple test if the change is greater than 0
   * @note These should be testable units of logic
   * @param df Incoming DataFrame with expected column `change`
   * @return DataFrame with new column `is_trending_up`
   */
  def is_trending_up(df: DataFrame): DataFrame = {
    df.withColumn("is_trending_up", when(col("change").gt(0.000f), true).otherwise(false))
  }

  /**
   * Like above, this could also perform lookups, regex to break up strings or other "simple" transforms
   * @note Further protections can be added to only monkey patch a named Dataset rather than a generic DataFrame
   * @param df Incoming data to be transformed. Requires column `volume` and `price`
   * @return DataFrame with new column `market_cap`
   */
  def market_cap(df: DataFrame): DataFrame = {
    df.withColumn("market_cap", col("volume") * col("price"))
  }

  implicit class SparkSessionWithPriceCalculations(df: DataFrame) {
    def withTrending(): DataFrame = {
      is_trending_up(df)
    }

    def withMarketCap(): DataFrame = {
      market_cap(df)
    }
  }

}

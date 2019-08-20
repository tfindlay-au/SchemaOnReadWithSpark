package MyDataProducts.enriched

import org.apache.spark.sql.DataFrame

/**
 * This object provides common logic used to enrich some data
 *
 * These are often used in the production of `features` or can be used by consumers if/when needed.
 */
object PriceCalculations {

  /**
   * This method performs a simple calculation on an input column.
   * @note These should be testable units of logic
   * @param df Incoming DataFrame with expected column
   * @return
   */
  def GSTFunction(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._
    import org.apache.spark.sql.functions.explode

    df.withColumn("original_price", explode($"menu.menu_items.price"))
      .withColumn("priceWithGST", $"original_price" * 1.1)
      .drop("original_price")
  }

  /**
   * Like above, this could also perform lookups, regex to break up strings or other "simple" transforms
   * @note Further protections can be added to only monkey patch a named Dataset rather than a generic DataFrame
   * @param df Incoming data to be transformed.
   * @return DataFrame with new column `priceWithCredCard`
   */
  def CreditCardFeesFunction(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._
    df.withColumn("priceWithCredCard", $"menu.menu_items.price" * 0.012)
  }

  implicit class SparkSessionWithPriceCalculations(df: DataFrame) {
    def withGST(): DataFrame = {
      GSTFunction(df)
    }

    def withCreditCardFees(): DataFrame = {
      CreditCardFeesFunction(df)
    }
  }

}

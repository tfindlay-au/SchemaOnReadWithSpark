package MyDataProducts.enriched

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{BooleanType, FloatType, StringType, StructField, StructType}
import org.scalatest.MustMatchers._
import org.scalatest.{Matchers, WordSpec}

object PriceCalculationsTest {

}

class PriceCalculationsTest extends WordSpec with Matchers with DataFrameSuiteBase {

  // TODO Push fixture into a trait perhaps ?
  def productRawStockSchema = StructType(List(
      StructField("stock_code", StringType, nullable = true),
      StructField("company_name", StringType, nullable = true),
      StructField("price", FloatType, nullable = true),
      StructField("change", FloatType, nullable = true),
      StructField("volume", FloatType, nullable = true)
  ))

  "PriceCalculations.is_trending_up" must {
    "be true if change > 0 column" in {
      val spark = SparkSession.builder.getOrCreate()

      // Given an input data frame ...
      val inputData = Seq(Row("ABC", "A Better Company", 100.0f, 0.001f, 10000000f))
      val inputDataFrame = spark.createDataFrame(spark.sparkContext.parallelize(inputData), productRawStockSchema)

      // When we call the function ...
      val resultDataFrame = PriceCalculations.is_trending_up(inputDataFrame)

      // Then the result should be ...
      val expectedData = Seq(Row("ABC", "A Better Company", 100.0f, 0.001f, 10000000f, true))
      val newSchema = productRawStockSchema.add(StructField("is_trending_up", BooleanType, nullable = false))
      val expectedDataFrame = spark.createDataFrame(spark.sparkContext.parallelize(expectedData), newSchema)

      assertDataFrameEquals(expectedDataFrame, resultDataFrame)
    }

    "be false if change < 0 column" in {
      val spark = SparkSession.builder.getOrCreate()

      // Given an input data frame ...
      val inputData = Seq(Row("ABC", "A Better Company", 100.0f, -0.001f, 10000000f))
      val inputDataFrame = spark.createDataFrame(spark.sparkContext.parallelize(inputData), productRawStockSchema)
      inputDataFrame.show()

      // When we call the function ...
      val resultDataFrame = PriceCalculations.is_trending_up(inputDataFrame)
      resultDataFrame.show()

      // Then the result should be ...
      val expectedData = Seq(Row("ABC", "A Better Company", 100.0f, -0.001f, 10000000f, false))
      val newSchema = productRawStockSchema.add(StructField("is_trending_up", BooleanType, nullable = false))
      val expectedDataFrame = spark.createDataFrame(spark.sparkContext.parallelize(expectedData), newSchema)
      expectedDataFrame.show()

      assertDataFrameEquals(expectedDataFrame, resultDataFrame)
    }
  }

  "PriceCalculations.market_cap" must {


  }
}

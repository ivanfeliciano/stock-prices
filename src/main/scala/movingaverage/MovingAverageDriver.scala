package movingaverage

import constants.Schemas.stockPricesSchema
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

class MovingAverageDriver(inputFilePath: String) {
  val schema = stockPricesSchema;
  val spark = SparkSession
    .builder
    .master("local[*]")
    .appName("MovingAverageStockPrices")
    .getOrCreate()

  def filteredByTickers(ticker: String, df: DataFrame) : DataFrame = {
    return df
      .select("ticker", "close", "date")
      .where(col("ticker") === ticker)
      .orderBy("date")
  }
  def run() = {
    println(inputFilePath)
    val stock_prices_df = spark.read.schema(schema)
      .option("header", "true")
      .csv(inputFilePath)
    val tickers = "GOOGL"
    val filtered = filteredByTickers(tickers, stock_prices_df)
    filtered.show(10)
  }
}

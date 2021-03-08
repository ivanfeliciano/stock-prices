package movingaverage

import constants.Schemas.stockPricesSchema
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{avg, col}

class MovingAverageDriver(inputFilePath: String, outputFilePath: String) {
  val schema = stockPricesSchema;
  val spark = SparkSession
    .builder
    .master("local[*]")
    .appName("MovingAverageStockPrices")
    .getOrCreate()

  def filteredByTickers(tickerList: List[String], df: DataFrame) : DataFrame =
    df
      .filter(col("ticker").isin(tickerList:_*))
      .orderBy("date")
      .coalesce(10)

  def calculate_moving_average(df: DataFrame, start: Int, end: Int): DataFrame = {
    val w = Window
      .partitionBy("ticker")
      .orderBy("ticker")
      .rowsBetween(start, end)
    df.withColumn("moving_avg", avg("close") over w)
  }
  def write_results(parquetPath: String, df: DataFrame): Unit =
    df.write.format("parquet").save(parquetPath)

  def run() = {
    println(inputFilePath)
    val stock_prices_df = spark.read.schema(schema)
      .option("header", "true")
      .csv(inputFilePath)
    val tickers = List("GOOGL", "FB", "AMZN")
    val filtered = filteredByTickers(tickers, stock_prices_df)
    val df_with_mov_avg = calculate_moving_average(filtered, 0, 6)
    write_results(outputFilePath, df_with_mov_avg)
  }
}

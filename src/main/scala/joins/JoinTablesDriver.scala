package joins

import constants.Schemas._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.broadcast

class JoinTablesDriver(tickerPricesPath: String, tickerNamesPath: String) {
  val prices_schema = stockPricesSchema
  val tickers_schema = tickerSchema
  val spark = SparkSession
    .builder
    .master("local[*]")
    .appName("MovingAverageStockPrices")
    .getOrCreate()

  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

  def broadcastJoin(prices_df: DataFrame, tickers_df: DataFrame): DataFrame =
    prices_df.join(broadcast(tickers_df), "ticker")

  def shuffleSortMergeJoin(prices_df: DataFrame, tickers_df: DataFrame): DataFrame =
    prices_df.join(tickers_df, "ticker")
    
  def run() = {
    val prices_df = spark.read.schema(stockPricesSchema)
      .option("header", "true")
      .csv(tickerPricesPath)
    val ticker_df = spark.read.schema(tickerSchema)
      .option("header", "true")
      .csv(tickerNamesPath)
    val broadcast_df = broadcastJoin(prices_df, ticker_df)
    val merge_join = shuffleSortMergeJoin(prices_df, ticker_df)
    println(broadcast_df.explain("extended"))
    println(merge_join.explain("extended"))
    spark.time(broadcast_df.show(20))
    spark.time(merge_join.show(20))
    Thread.sleep(300000)
  }
  
}

package constants

import org.apache.spark.sql.types._

object Schemas {
  final val stockPricesSchema = StructType(Array(
    StructField("ticker", StringType, true),
    StructField("open", FloatType, true),
    StructField("close", FloatType, true),
    StructField("adj_close", FloatType, true),
    StructField("low", FloatType, true),
    StructField("high", FloatType, true),
    StructField("volume", IntegerType, true),
    StructField("date", DateType, true)
  ))
  final val tickerSchema = StructType(Array(
    StructField("ticker", StringType, true),
    StructField("exchange", StringType, true),
    StructField("name", StringType, true),
    StructField("sector", StringType, true),
    StructField("industry", StringType, true)
  ))
}

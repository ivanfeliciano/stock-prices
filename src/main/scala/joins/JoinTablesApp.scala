package joins

object JoinTablesApp extends App {
  val driver = new JoinTablesDriver("data/historical_stock_prices.csv",
  "data/historical_stocks.csv")
  driver.run()
}

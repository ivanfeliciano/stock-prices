package movingaverage

object MovingAverageApp extends App {
  val driver = new MovingAverageDriver("data/historical_stock_prices.csv")
  driver.run()
}

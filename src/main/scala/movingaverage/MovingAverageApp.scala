package movingaverage

object MovingAverageApp extends App {
  val driver = new MovingAverageDriver("data/historical_stock_prices.csv",
    "data/moving_avg_fb_amzn_googl")
  driver.run()
}

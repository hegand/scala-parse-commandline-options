package parser
import org.apache.log4j.{Level, LogManager, Logger}
import scala.math.random
import scala.io.Source
import org.apache.spark.sql.SparkSession
import scopt.OptionParser

case class JobConfiguration(job: String = null,
                            days: String = null,
                            slices: Int = 2,
                            configPath: String = null)

object Parser {

  val cmdLineParser = new OptionParser[JobConfiguration]("PARSER") {
    head("PARSER")

    opt[String]("job")
      .text("Job Name")
      .action((x, config) => config.copy(job = x))

    opt[String]("days")
      .text("Date Range")
      .required()
      .action((x, config) => config.copy(days = x))

    opt[Int]("slices")
      .text("slices")
      .required()
      .action((x, config) => config.copy(slices = x))

    opt[String]("config")
      .text("Config Path")
      .action((x, config) => config.copy(configPath = x))

    help("help").text("prints this help text")

    checkConfig( c =>
      if (c.days == None) {
        failure("dates must be specified")
      } else success
    )
  }

  def main(args: Array[String]) {
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    //Logger.getLogger("org").setLevel(Level.OFF)
    //Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession.builder().getOrCreate()
    log.info("Spark version " + (spark.version))

    cmdLineParser.parse(args, JobConfiguration()) map { jobConfig =>
      log.info("Starting PARSER...")
      log.info(s"config = $jobConfig")
      log.info("job = " + jobConfig.job)

      for (line <- Source.fromFile(jobConfig.configPath).getLines) {
        log.info(line)
      }

      /** Computes an approximation to pi
        * From https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/SparkPi.scala
        * */
      val n = math.min(100000L * jobConfig.slices, Int.MaxValue).toInt
      val count = spark.sparkContext.parallelize(1 until n, jobConfig.slices).map { i =>
        val x = random * 2 - 1
        val y = random * 2 - 1
        if (x*x + y*y <= 1) 1 else 0
      }.reduce(_ + _)
      log.info(s"Pi is roughly ${4.0 * count / (n - 1)}")

    } getOrElse {

    }
    spark.stop()
  }
}

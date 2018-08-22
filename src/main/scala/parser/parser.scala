package parser
import org.apache.log4j.{Level, LogManager, Logger}

import scala.io.Source
import org.apache.spark.sql.SparkSession
import scopt.OptionParser

case class JobConfiguration(job: String = null,
                            days: String = null,
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
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession.builder().getOrCreate()
    log.info("Spark version " + (spark.version))

    cmdLineParser.parse(args, JobConfiguration()) map { jobConfig =>
      log.info("Starting PARSER...")
      log.info(s"config = $jobConfig")
      log.info("job = " + jobConfig.job)

      for (line <- Source.fromFile(jobConfig.configPath).getLines) {
        log.warn(line)
      }

      val t = spark.read.textFile(jobConfig.configPath)
      log.info("number of rows in config file = " + t.count())

    } getOrElse {

    }
  }
}

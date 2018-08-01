package parser
import org.apache.log4j.{Level, LogManager}

import scala.io.Source
import org.apache.spark.sql.SparkSession
import scopt.OptionParser

case class JobConfiguration(job: String = null,
                            dates: String = null,
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
      .action((x, config) => config.copy(dates = x))

    opt[String]("config")
      .text("Config Path")
      .action((x, config) => config.copy(configPath = x))

    help("help").text("prints this help text")
  }

  def main(args: Array[String]) {
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)

    val spark = SparkSession.builder().getOrCreate()
    log.info("Spark version " + (spark.version))

    cmdLineParser.parse(args, JobConfiguration()) map { jobConfig =>
      log.warn("Starting PARSER...")
      log.warn(s"config = $jobConfig")

      for (line <- Source.fromFile(jobConfig.configPath).getLines) {
        log.warn(line)
      }

    } getOrElse {

    }
  }
}

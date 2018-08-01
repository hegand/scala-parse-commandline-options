# SPARK PARSE COMMANDLINE OPTIONS

##Usage

```shell
SPARK_MAJOR_VERSION=2 spark-submit --master yarn-cluster --jars scopt_2.11-3.7.0.jar --files test --class parser.Parser spark-parse-commandline-options_2.11-0.1.jar --days 2 --job test --config test
```
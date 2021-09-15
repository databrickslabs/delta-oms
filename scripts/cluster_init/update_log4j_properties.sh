#!/usr/bin/bash

set -e

cat << EOF >> /databricks/spark/dbconf/log4j/driver/log4j.properties

# OMS Custom Logger Configuration
log4j.logger.com.databricks.labs.deltaoms=INFO, oms
log4j.additivity.com.databricks.labs.deltaoms=false
log4j.appender.oms=com.databricks.logging.RedactionRollingFileAppender
log4j.appender.oms.layout=org.apache.log4j.PatternLayout
log4j.appender.oms.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n
log4j.appender.oms.rollingPolicy=org.apache.log4j.rolling.TimeBasedRollingPolicy
log4j.appender.oms.rollingPolicy.FileNamePattern=logs/log4j.oms-%d{yyyy-MM-dd-HH}.log.gz
log4j.appender.oms.rollingPolicy.ActiveFileName=logs/stdout.oms-active.log

EOF
_Copyright &copy; Cloudera, Inc. 2018_
# Spark Structured Streaming reference application for CDH

## Introduction

This project includes sample applications that demonstrate an Apache Kafka -> Apache Spark Structured Streaming -> Apache Kudu pipeline for ingestion.

Please check out the [documentation](docs/doc.md) to get an overview of building Spark structured streaming applications on the CDH platform,
a description of the use case the application solves, the components and the integration techniques used to realize a simple
streaming system using the Cloudera stack.  

There are two sample applications implementing a streaming application in two different ways.

* The simpleApp focuses on the integration aspect:
it demonstrates the simplest way to connect Spark with Kafka and Kudu.
* The advancedApp also shows a way to abstract out the business logic from the application.
It enables easy switching between various sources and sinks and eases testing on different levels.   

The applications demonstrate some basic Structured Streaming techniques, including stream - static table join to enrich data in the incoming stream
and windowing.

For the preparation and execution instructions please see the README file of the separate projects.

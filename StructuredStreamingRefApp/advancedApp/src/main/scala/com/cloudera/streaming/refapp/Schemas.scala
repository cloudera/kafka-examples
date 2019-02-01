/**
  * Copyright (C) Cloudera, Inc. 2019
  */
package com.cloudera.streaming.refapp

import java.lang.reflect.Type
import java.util

import com.google.gson._
import org.apache.kafka.common.serialization.Serializer

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types.{StringType, StructType}

/**
  * Contains schemas and serializers for various domain objects.
  */
object Schemas {
  val timestampColumnName = "timestamp"
  val transaction: StructType = Encoders.product[Transaction].schema
  val vendor: StructType = Encoders.product[Vendor].schema
  val customer: StructType = Encoders.product[Customer].schema

  val state: StructType = Encoders.product[State].schema
  val plainText: StructType = new StructType().
    add("value", StringType)
}

class OptionSerializer extends JsonSerializer[Option[Any]] {
  override def serialize(src: Option[Any], typeOfSrc: Type, context: JsonSerializationContext): JsonElement = {
    src match {
      case None => JsonNull.INSTANCE
      case Some(v) => context.serialize(v)
    }
  }
}

class TransactionSerializer extends Serializer[Transaction] {

  private val gson = new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss").registerTypeHierarchyAdapter(classOf[Option[Any]], new OptionSerializer).create()

  override def configure(map: util.Map[String, _], b: Boolean): Unit = {}

  override def serialize(topic: String, transaction: Transaction): Array[Byte] = {
    gson.toJson(transaction).getBytes
  }

  override def close(): Unit = {}
}

class CustomerSerializer extends Serializer[Customer] {

  private val gson = new GsonBuilder().setDateFormat("yyyy-MM-dd").create()

  override def configure(map: util.Map[String, _], b: Boolean): Unit = {}

  override def serialize(topic: String, customer: Customer): Array[Byte] = {
    gson.toJson(customer).getBytes
  }

  override def close(): Unit = {}
}

class VendorSerializer extends Serializer[Vendor] {

  private val gson = new GsonBuilder().setDateFormat("yyyy-MM-dd").create()

  override def configure(map: util.Map[String, _], b: Boolean): Unit = {}

  override def serialize(topic: String, vendor: Vendor): Array[Byte] = {
    gson.toJson(vendor).getBytes
  }

  override def close(): Unit = {}
}

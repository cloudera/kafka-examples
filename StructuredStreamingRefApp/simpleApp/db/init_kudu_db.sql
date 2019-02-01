/**
 * Copyright (C) Cloudera, Inc. 2019
 */

CREATE DATABASE IF NOT EXISTS streaming_ref;
USE streaming_ref;

DROP TABLE IF EXISTS customers;
CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    first_name STRING,
    last_name STRING,
    state_name STRING,
    state_abbreviation STRING,
    update_timestamp TIMESTAMP)
PARTITION BY HASH (customer_id) PARTITIONS 10
STORED AS KUDU TBLPROPERTIES ('kudu.num_tablet_replicas' = '3');

DROP TABLE IF EXISTS transactions;
CREATE TABLE transactions (
    event_timestamp TIMESTAMP,
    transaction_id STRING,
    vendor_id INT,
    event_state STRING,
    price STRING,
    card_type STRING,
    customer_id INT,
    customer_first_name STRING,
    customer_last_name STRING,
    is_valid BOOLEAN,
    PRIMARY KEY (event_timestamp, transaction_id)
)
PARTITION BY
    HASH (transaction_id) PARTITIONS 15,
    RANGE (event_timestamp)
      (PARTITION '2018-11-01' <= VALUES < '2018-12-01')
STORED AS KUDU TBLPROPERTIES ('kudu.num_tablet_replicas' = '1');
ALTER TABLE transactions ADD RANGE PARTITION '2018-12-01' <= VALUES < '2019-01-01';
ALTER TABLE transactions ADD RANGE PARTITION '2019-01-01' <= VALUES < '2019-02-01';
ALTER TABLE transactions ADD RANGE PARTITION '2019-02-01' <= VALUES < '2019-03-01';
-- ...

DROP TABLE IF EXISTS operational_metadata;
CREATE TABLE operational_metadata(
    start_ts TIMESTAMP PRIMARY KEY,
    end_ts TIMESTAMP,
    num_transactions BIGINT)
STORED AS KUDU TBLPROPERTIES ('kudu.num_tablet_replicas' = '3');

insert into customers values (1, 'John', 'Doe', 'Alabama', 'AL', '2018-01-01');
insert into customers values (2, 'Jane', 'Miller', 'Alaska', 'AK', '2018-01-01');
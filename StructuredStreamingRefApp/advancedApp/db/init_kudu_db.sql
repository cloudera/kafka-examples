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

DROP TABLE IF EXISTS vendors;
CREATE TABLE vendors (
    vendor_id INT PRIMARY KEY,
    vendor_name STRING,
    phone_number STRING,
    update_timestamp TIMESTAMP)
PARTITION BY HASH (vendor_id) PARTITIONS 10
STORED AS KUDU TBLPROPERTIES ('kudu.num_tablet_replicas' = '3');

DROP TABLE IF EXISTS states;
CREATE TABLE states (
    state_id INT PRIMARY KEY,
    state_name STRING,
    state_abbreviation STRING)
STORED AS KUDU TBLPROPERTIES ('kudu.num_tablet_replicas' = '3');

DROP TABLE IF EXISTS valid_transactions;
CREATE TABLE valid_transactions (
    event_timestamp TIMESTAMP,
    transaction_id STRING,
    customer_id INT,
    vendor_id INT,
    event_state STRING,
    price STRING,
    card_type STRING,
    PRIMARY KEY (event_timestamp, transaction_id)
)
PARTITION BY
    HASH (transaction_id) PARTITIONS 15,
    RANGE (event_timestamp)
      (PARTITION '2018-11-01' <= VALUES < '2018-12-01')
STORED AS KUDU TBLPROPERTIES ('kudu.num_tablet_replicas' = '3');
ALTER TABLE valid_transactions ADD RANGE PARTITION '2018-12-01' <= VALUES < '2019-01-01';
ALTER TABLE valid_transactions ADD RANGE PARTITION '2019-01-01' <= VALUES < '2019-02-01';
ALTER TABLE valid_transactions ADD RANGE PARTITION '2019-02-01' <= VALUES < '2019-03-01';
-- ...

DROP TABLE IF EXISTS invalid_transactions;
CREATE TABLE invalid_transactions (
    transaction_id STRING PRIMARY KEY,
    customer_id INT,
    vendor_id INT,
    event_state STRING,
    event_timestamp TIMESTAMP,
    price STRING,
    card_type STRING)
PARTITION BY
    HASH (transaction_id) PARTITIONS 15
STORED AS KUDU TBLPROPERTIES ('kudu.num_tablet_replicas' = '3');

DROP TABLE IF EXISTS customer_orphans;
CREATE TABLE customer_orphans (
    customer_id INT PRIMARY KEY,
    first_name STRING,
    last_name STRING,
    state_id INT,
    update_timestamp TIMESTAMP)
STORED AS KUDU TBLPROPERTIES ('kudu.num_tablet_replicas' = '3');

DROP TABLE IF EXISTS vendor_orphans;
CREATE TABLE vendor_orphans (
    vendor_id INT PRIMARY KEY,
    vendor_name STRING,
    phone_number STRING,
    update_timestamp TIMESTAMP)
STORED AS KUDU TBLPROPERTIES ('kudu.num_tablet_replicas' = '3');

DROP TABLE IF EXISTS transactions_operational_metadata;
CREATE TABLE transactions_operational_metadata(
    start_ts TIMESTAMP PRIMARY KEY,
    end_ts TIMESTAMP,
    num_transactions BIGINT)
STORED AS KUDU TBLPROPERTIES ('kudu.num_tablet_replicas' = '3');

insert into customers values (1, 'John', 'Doe', 'Alabama', 'AL', '2018-01-01');
insert into customers values (2, 'Jane', 'Miller', 'Alaska', 'AK', '2018-01-01');

insert into vendors values (1, 'Apple', '123456', '2018-11-13');
insert into vendors values (2, 'Dell', '345678', '2018-11-13');

INSERT INTO states values (-1, 'Unknown', '??');
INSERT INTO states values (1, 'Alabama', 'AL');
INSERT INTO states values (2, 'Alaska', 'AK');
INSERT INTO states values (3, 'Arizona', 'AZ');
INSERT INTO states values (4, 'Arkansas', 'AR');
INSERT INTO states values (5, 'California', 'CA');
INSERT INTO states values (6, 'Colorado', 'CO');
INSERT INTO states values (7, 'Connecticut', 'CT');
INSERT INTO states values (8, 'Delaware', 'DE');
INSERT INTO states values (9, 'District of Columbia', 'DC');
INSERT INTO states values (10, 'Florida', 'FL');
INSERT INTO states values (11, 'Georgia', 'GA');
INSERT INTO states values (12, 'Hawaii', 'HI');
INSERT INTO states values (13, 'Idaho', 'ID');
INSERT INTO states values (14, 'Illinois', 'IL');
INSERT INTO states values (15, 'Indiana', 'IN');
INSERT INTO states values (16, 'Iowa', 'IA');
INSERT INTO states values (17, 'Kansas', 'KS');
INSERT INTO states values (18, 'Kentucky', 'KY');
INSERT INTO states values (19, 'Louisiana', 'LA');
INSERT INTO states values (20, 'Maine', 'ME');
INSERT INTO states values (21, 'Maryland', 'MD');
INSERT INTO states values (22, 'Massachusetts', 'MA');
INSERT INTO states values (23, 'Michigan', 'MI');
INSERT INTO states values (24, 'Minnesota', 'MN');
INSERT INTO states values (25, 'Mississippi', 'MS');
INSERT INTO states values (26, 'Missouri', 'MO');
INSERT INTO states values (27, 'Montana', 'MT');
INSERT INTO states values (28, 'Nebraska', 'NE');
INSERT INTO states values (29, 'Nevada', 'NV');
INSERT INTO states values (30, 'New Hampshire', 'NH');
INSERT INTO states values (31, 'New Jersey', 'NJ');
INSERT INTO states values (32, 'New Mexico', 'NM');
INSERT INTO states values (33, 'New York', 'NY');
INSERT INTO states values (34, 'North Carolina', 'NC');
INSERT INTO states values (35, 'North Dakota', 'ND');
INSERT INTO states values (36, 'Ohio', 'OH');
INSERT INTO states values (37, 'Oklahoma', 'OK');
INSERT INTO states values (38, 'Oregon', 'OR');
INSERT INTO states values (39, 'Pennsylvania', 'PA');
INSERT INTO states values (40, 'Rhode Island', 'RI');
INSERT INTO states values (41, 'South Carolina', 'SC');
INSERT INTO states values (42, 'South Dakota', 'SD');
INSERT INTO states values (43, 'Tennessee', 'TN');
INSERT INTO states values (44, 'Texas', 'TX');
INSERT INTO states values (45, 'Utah', 'UT');
INSERT INTO states values (46, 'Vermont', 'VT');
INSERT INTO states values (47, 'Virginia', 'VA');
INSERT INTO states values (48, 'Washington', 'WA');
INSERT INTO states values (49, 'West Virginia', 'WV');
INSERT INTO states values (50, 'Wisconsin', 'WI');
INSERT INTO states values (51, 'Wyoming', 'WY');

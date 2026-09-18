CREATE DATABASE IF NOT EXISTS shop;
USE shop;

CREATE TABLE IF NOT EXISTS orders (
    id      BIGINT      NOT NULL PRIMARY KEY,
    v       VARCHAR(128) NOT NULL,
    amount  DOUBLE      NULL
);

CREATE TABLE IF NOT EXISTS order_items (
    order_id BIGINT NOT NULL,
    line_no  INT    NOT NULL,
    sku      VARCHAR(64) NOT NULL,
    qty      INT    NOT NULL,
    PRIMARY KEY (order_id, line_no)
);

-- Temporal parity fixture (issue #139): a naive DATETIME and an instant
-- TIMESTAMP, used to prove the snapshot and CDC paths agree under a
-- non-UTC `timezone`.
CREATE TABLE IF NOT EXISTS events (
    id          BIGINT       NOT NULL PRIMARY KEY,
    happened_at DATETIME(6)  NULL,
    recorded_at TIMESTAMP(6) NULL
);

CREATE USER IF NOT EXISTS 'repl'@'%' IDENTIFIED BY 'replpass';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'repl'@'%';
GRANT SELECT ON shop.* TO 'repl'@'%';
FLUSH PRIVILEGES;
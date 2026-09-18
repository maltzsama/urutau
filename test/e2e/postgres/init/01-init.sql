CREATE TABLE IF NOT EXISTS orders (
    id      BIGINT           NOT NULL PRIMARY KEY,
    v       TEXT             NOT NULL,
    amount  DOUBLE PRECISION NULL,
    active  BOOLEAN          NOT NULL DEFAULT true
);

-- A range-partitioned table with two leaves, for the CTID partitioned-chunk
-- path (#151). The parent has no storage; the leaves hold the pages.
CREATE TABLE IF NOT EXISTS orders_part (
    id      BIGINT           NOT NULL,
    v       TEXT             NOT NULL,
    amount  DOUBLE PRECISION NULL,
    active  BOOLEAN          NOT NULL DEFAULT true,
    PRIMARY KEY (id)
) PARTITION BY RANGE (id);

CREATE TABLE IF NOT EXISTS orders_part_0 PARTITION OF orders_part FOR VALUES FROM (0) TO (1000);
CREATE TABLE IF NOT EXISTS orders_part_1 PARTITION OF orders_part FOR VALUES FROM (1000) TO (2000);

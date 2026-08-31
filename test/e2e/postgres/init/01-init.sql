CREATE TABLE IF NOT EXISTS orders (
    id      BIGINT           NOT NULL PRIMARY KEY,
    v       TEXT             NOT NULL,
    amount  DOUBLE PRECISION NULL,
    active  BOOLEAN          NOT NULL DEFAULT true
);

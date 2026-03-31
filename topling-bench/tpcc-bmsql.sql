SELECT count(*) AS low_stock FROM (
    SELECT s_w_id, s_i_id, s_quantity
        FROM bmsql_stock
        WHERE s_w_id = ? AND s_quantity < ? AND s_i_id IN (
            SELECT ol_i_id
                FROM bmsql_district
                JOIN bmsql_order_line ON ol_w_id = d_w_id
                 AND ol_d_id = d_id
                 AND ol_o_id >= d_next_o_id - 20
                 AND ol_o_id < d_next_o_id
                WHERE d_w_id = ? AND d_id = ?
        )
    ) AS L

CREATE TABLE bmsql_order_line (
  ol_w_id int NOT NULL,
  ol_d_id int NOT NULL,
  ol_o_id int NOT NULL,
  ol_number int NOT NULL,
  ol_i_id int NOT NULL,
  ol_delivery_d timestamp NULL DEFAULT NULL,
  ol_amount decimal(10,2) DEFAULT NULL,
  ol_supply_w_id int DEFAULT NULL,
  ol_quantity int DEFAULT NULL,
  ol_dist_info char(24) DEFAULT NULL,
  PRIMARY KEY (ol_w_id,ol_d_id,ol_o_id,ol_number)
)

EXPLAIN SELECT ol_i_id
    FROM bmsql_district
    JOIN bmsql_order_line ON ol_w_id = d_w_id
     AND ol_d_id = d_id
     AND ol_o_id >= d_next_o_id - 20
     AND ol_o_id < d_next_o_id
    WHERE d_w_id = 1 AND d_id = 5;

 CREATE TABLE bmsql_oorder (
  o_w_id int NOT NULL,
  o_d_id int NOT NULL,
  o_id int NOT NULL,
  o_c_id int DEFAULT NULL,
  o_carrier_id int DEFAULT NULL,
  o_ol_cnt int DEFAULT NULL,
  o_all_local int DEFAULT NULL,
  o_entry_d timestamp NULL DEFAULT NULL,
  PRIMARY KEY (o_w_id,o_d_id,o_id),
  UNIQUE KEY bmsql_oorder_idx1 (o_w_id,o_d_id,o_carrier_id,o_id)
)
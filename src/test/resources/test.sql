CREATE TABLE kudu.s1.t7
(
    c1 int WITH (primary_key = true)
)
WITH (
    partition_by_hash_columns = ARRAY['c1'],
    partition_by_hash_buckets = 2
);
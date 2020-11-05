CREATE TABLE kudu.s1.t10
(
    c1 int WITH (primary_key = true),
    c2 double with(nullable= true)
)
WITH (
    partition_by_hash_columns = ARRAY['c1'],
    partition_by_hash_buckets = 2
);

CREATE TABLE kudu.s1.t1
(
    c1  int WITH (primary_key = true),
    sc2 decimal(10, 5) with(nullable= true)
)
WITH (
    partition_by_hash_columns = ARRAY['c1'],
    partition_by_hash_buckets = 2
);


create table kudu.s1.execution_jobs
(
    exec_id       int WITH (primary_key = true),
    job_id        varchar WITH (primary_key = true),
    flow_id       varchar WITH (primary_key = true),
    attempt       int WITH (primary_key = true),
    project_id    int,
    version       int,
    start_time    bigint WITH(nullable = true),
    end_time      bigint WITH (nullable = true),
    status        tinyint WITH(nullable = true),
    input_params  VARBINARY WITH (nullable = true),
    output_params VARBINARY WITH (nullable = true),
    attachments   VARBINARY WITH (nullable = true)
)
WITH (
    partition_by_hash_columns = ARRAY['exec_id','job_id','flow_id','attempt'],
    partition_by_hash_buckets = 2
);

create index ex_job_id
    on execution_jobs (project_id, job_id);


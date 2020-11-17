https://repository.sonatype.org/service/local/repositories/central-proxy/content/com/alibaba/fastjson/1.2.74/fastjson-1.2.74.jar
https://repository.sonatype.org/service/local/repositories/central-proxy/content/org/apache/commons/commons-lang3/3.10/commons-lang3-3.10.jar
https://repository.sonatype.org/service/local/repositories/central-proxy/content/org/apache/commons/commons-email/1.5/commons-email-1.5.jar
https://repository.sonatype.org/service/local/repositories/central-proxy/content/commons-io/commons-io/2.8.0/commons-io-2.8.0.jar
CREATE TABLE kudu.s1.t10
(
    c1 int WITH (primary_key = true),
    c2 double with(nullable= true)
)
WITH (
    partition_by_hash_columns = ARRAY['c1'],
    partition_by_hash_buckets = 2
);

CREATE TABLE kudu.s1.t103
(
    id int WITH (primary_key = true),
    t1 timestamp,
    t2 timestamp
)
WITH (
    partition_by_hash_columns = ARRAY['id'],
    partition_by_hash_buckets = 10
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


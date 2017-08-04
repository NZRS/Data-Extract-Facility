# Database


## The def schema

On the target database, we create a schema called 'def'. This stores a list of jobs that have run, and the result of each run.


### The def.jobs table

This table records the jobs in the system. A row is automatically added by the DEF processor when the job is run for the first time. It's main function is to track when the job can next be run again. If a job fails to run successfully, this table is not modified (meaning the job will be tried again next time it runs).

| Column | Type | Note |
| --- | --- | --- |
| id | BIGSERIAL | Primary key. |
| input\_file | TEXT UNIQUE NOT NULL | Will be the full file name including directory (relative to the --directory option). |
| last\_run | TIMESTAMP WITH TIME ZONE NOT NULL | The time that the job successfully ran. |
| next\_run | TIMESTAMP WITH TIME ZONE | The time the job is scheduled to run next. Can be NULL if frequency is 'adhoc'. |


### The def.jobs\_run table

This table represents on run of the job. The results table (below) represents each time that run is attempted, which could be multiple times if their is a failure.

| Column | Type | Note |
| --- | --- | --- |
| id | UUID | Primary key. |
| job\_id | BIGINT | References the jobs table above. |
| placeholder\_date | ARRAY | The placeholder date used for this run. |
| output\_file | TEXT | The name of the file that was written or sent as an e-mail attachment. |

### The def.jobs\_run\_result table

This table records each time a job run is executed, regardless if it succeeded or failed. The main purpose of this table is to have a record of what was recorded, and maintain the status of failing jobs.id.

| Column | Type | Note |
| --- | --- | --- |
| id | BIGSERIAL | Primary key. |
| job\_run\_id | BIGINT | References the job id above. |
| started | TIMESTAMP WITH TIMEZONE NOT NULL | Time the job started. |
| finished | TIMESTAMP WITH TIMEZONE NOT NULL | Time the job completed. |
| rows\_recorded | INTEGER | The number of rows in the result. NULL only on failure. |
| error | TEXT | The error message (if any). |


# YAML specification for configuration file

The configuration of DEF runner is done in a YAML configuration file. The location of the file is specified as a command line option. All keys are prefixed with the hash def. Please see the examples page for an example YAML file.

## Database connection details

These values sit under the 'db' key. This is a hash of database names and their connection details. The key is the name used as 'source_db' and 'target_db' in individual jobs.

While no options are mandatory, you will need at least one database configuration for the system to work.

| Key name | Type | Note |
| --- | ---  | --- |
| type | Text | The database type, defaults to 'pg' (Postgres) if not specified. |
| user | Text | The database username, if required. |
| pass | Text | The database password, if required. |
| is\_source | Boolean | True if this database can be used as a source database. |
| is\_target | Boolean | True if this database can be used as a target database. |
| cmd\_to\_pause | String or Array | If set, will run this command rather than calling ``SELECT pg_xlog_replay_pause()`` if the database needs to be paused. |
| cmd\_to\_resume | String or Array | If set, will run this command rather than calling ``SELECT pg_xlog_replay_resume()`` if the database needs to be resumed. |
| * | Text | Anything else supplied will be passed directly to DBI. Examples include database or dbname or db, service, host, port. |

# Mail sending options.

These values sit under the 'mail' key. These options determine how DEF sends e-mail, as well as a default to or from address (which can be overridden in a job if needed). If type is 'sendmail', all other options are ignored.

| Key name | Type | Note |
| --- | --- | --- |
| from | Text | The default from address for e-mail sent (can be overridden on a per job basis). |
| to | Text | The default to address for sending e-mails (can be overridden on a per job basis). |
| type | Text | Either 'sendmail' or 'smtp'. 'sendmail' is assumed if not specified. |
| host | Text | The host name / IP address to use for SMTP delivery. |
| port | Integer | The SMTP port to use (default 25 for SMTP, 465 for SSL). |
| ssl | Boolean | Whether to use SSL to send e-mail. Default is false. |
| user | Text | The username for the SMTP server used to send e-mail. |
| pass | Text | The password for the SMTP server used to send e-mail. |

## Miscellaneous options
Finally, there are some options which sit directly under the 'def' element. They are:

| Key name | Type | Note |
| --- |  --- | --- |
| delay\_after\_failure | Integer | The number of seconds to wait after a job fails before trying again |
| directory | Text | The directory to process YAML files from. Can be overwritten with the --dir command line option. |
| output\_directory | Text | The directory where files will be stored if the output is 'file' |
| time_zone | Text | The timezone used for calculating relative dates, default is the system local time zone. Maybe required if DateTime::TimeZone cannot figure this automatically. |

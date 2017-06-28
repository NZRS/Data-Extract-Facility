# YAML specification for writing a job

Configuration of individual Data Extract jobs are done by writing a file in the [YAML](http://www.yaml.org/spec/1.2/spec.html) format. The technical details are on their website, but you might find the [Wikipedia page](https://en.wikipedia.org/wiki/YAML) easier reading. Alternatively, look at some examples (TODO: link to example page).

These are the keys that are supported:

| Key name | Type | Mandatory | Notes |
| --- | --- | --- | --- |
| name | String | Yes | A one line string that represents the name of job. |
| description | String | Yes | A more detailed description of what the job does. |
| frequency | String | Yes | See below. |
| source\_db | String | Yes | The name of the source database to connect to. |
| sql | String | Yes | The SQL statement that should be run. Placeholders values can be used for relative dates, see the section below for details. |
| pause | Boolean | No | For some really long running queries the database can get inconsistent with the always-coming replication stream. Pausing the replication may fix this issue. Default is false. |
| transform»rule | String | No | The transform rule to run. See transform rules (TODO: link) |
| transform»args | Hash | No | An additional arguments that the transformation rules requires. |
| target\_db | String | See notes | The name of the target database to stored results in. This is not required if your output is not a database, and the frequency is 'adhoc'. |
| output | String | Yes | One of 'file', 'email' or 'db'. |
| filename | String | when output is not 'db' | The filename to use. |
| format | String | No | Either 'YAML', 'JSON' or 'CSV'. Used for file and e-mail results. Default is CSV if not specified. |
|compress | Boolean | No | Will compress a file or e-mail before sending. Default is not to compress. Compression method is zip. |
| target\_table | String | when output is 'db' | The table that the results should be stored in. Required if output is not a file or e-mail. |
| target\_columns | Array | when output is 'db' | A list of column names that the results should be stored in. Required if output is not a file or e-mail. |
| target\_presql | String | No | Any SQL that needs to be run before inserting rows into the database |
| target\_postsql | String | No | Any SQL that needs to be run after inserting the rows into the database. |
| notify | Boolean | No | If true, you will receive an e-mail notifying you when the script has been processed. |
| retry\_on\_failure | Boolean | No | If true, will try reprocessing earlier versions of the job that failed. This is only run once a job is completed successfully again. |
| email»to | String or Array | No | Overrides the default e-mail address(es). This is the address(es) to send results to (if using e-mail output). It is also used to notify you if there are any errors. |
| email»from | String | No | Override the default from e-mail address. |
| email»subject | String | No | Override the default subject (default is Report: <name>) |

## Frequency value

This value tells the DEF how often the job should be run. It should start with the word 'adhoc', 'hourly', 'daily', 'weekly' or 'monthly', optionally followed by a qualify. You can specify that it should run after a particular time by appending 'after <time>', for example 'after 2pm'. For weekly jobs you can specify a day of the week with 'on <day>', for example 'on Sunday'. For monthly jobs, you can specify a day of the week, or 'on the <day of month>' or 'on or after the <day of month>', for example 'on the 15th'. Using both day of week and 'on or after the' means you can specify the day of month you want it to run. For example 'on Wednesday on or after the 8th' will run on the second Wednesday in the month.

A job will always run when it is first inserted into the DEF. It will then be scheduled to run as per the frequency specified. If 'never' is specified as the frequency, the query will only be run manually.

## SQL

Writing SQL is beyond the scope of this document. There are sites on the Internet on how to do it. You can always ask a friendly programmer to help you if you need assistance in writing the correct SQL statement.

### Placeholders values

Since we may want to run queries based on a relative date, you can use the following strings in your SQL to mention a date. The format will always be YYYY-MM-DD (without quotes). You may need to wrap this around a TO\_DATE, STR\_TO\_DATE or other date based function (Postgres does NOT need any wrapping). The following is true if today is May 30th, 2017:

| String | Description | Example |
| --- | --- | --- |
| $today$ | Today's date | 2017-05-30 |
| $yesterday$ | Yesterday's date | 2017-05-29 |
| $lastmonth$ | The 1st of the previous month | 2017-04-01 |
| $thismonth$ | The 1st of the current month | 2017-05-01 |
| $nextmonth$ | The 1st of the following month | 2017-06-01 |
| $thisfinyear$ | The start of the current financial year (always April 1st) | 2017-04-01 |
| $nextfinyear$ | The start of the next financial year | 2018-04-01 |

## Filename

If outputting the results to a file or e-mail, you can use [strftime](https://metacpan.org/pod/DateTime#strftime-Patterns) patterns to specify a dynamic file name. For example 'result-%F.csv', will generate a file called 'result-2017-05-30.csv' if run on May 30th, 2017.


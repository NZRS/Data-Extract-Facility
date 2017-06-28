# Data Extract Facility Code

This document details a bit about the code. If you are an end user of the Data Extract Facility, there is no need to read or understand this document to use it.

## Required modules

These are a list of Perl modules that are required for the DEF script, including the Ubuntu or Debian package name.

| Module Name | Ubuntu package name | Minimum version | Purpose |
| --- | --- | --- | --- |
| Archive::Zip | libarchive-zip-perl | | Compress files if the 'compress' option is used. |
| Data::UUID | libdata-uuid-perl |  | Generate UUIDS for job run ids. |
| DateTime | libdatetime-perl | | Date calculating functions. |
| DateTime::Format::Pg | libdatetime-format-pgperl | | Formatting of DateTime objects to Postgres format. |
| DateTime::Format::Strptime | libdatetime-format-strptime-perl | | Formatting of YYYY-MM-DD strings to DateTime objects. |
| DBD::Pg | libdbd-pg-perl | 3.5 [1] | Connect to the database. Versions < 3.5 don't support UTF-8 correctly. |
| Email::Sender::Transport::Sendmail | libemail-sender-perl | | Send e-mail via sendmail. |
| Email::Sender::Transport::SMTP | libemail-sender-perl [2] | | Send e-mail via SMTP. |
| Email::Stuffer | libemail-stuffer-perl | 0.015 [3] | Generate multipart e-mails. |
| File::Find::Rule | libfile-find-rule-perl | | Find all YAML files in a directory. |
| Find::Pid | libfind-pid-perl | | Check if the process is already running. |
| FindBin | perl-modules-5.XX | | Find the template directory. |
| File::Spec | perl-base | | Correct handling of concatenating directory and files together. |
| Getopt::Long | perl-base | | Parse command line options. |
| IO::String | libio-string-perl | | Required for Archive::Zip to write to scalars. |
| JSON::XS | libjson-xs-perl | | Write JSON files. Fast. |
| List::MoreUtils | liblist-moreutils-perl | | For the 'any' and 'mesh' fuctions. |
| POSIX | perl-base | | For the fmod function. |
| Template | libtemplate-perl | | Generating text and HTML e-mails. |
| Text::CSV_XS | libtext-csv-xs-perl | 1.23 | Write CSV data. Fast. |
| Time::HiRes | libperl5.XX | | Because DateTime->now() is only to the second :( |
| TryCatch | libtrycatch-perl | | Capture errors. |
| YAML::Syck | libyaml-syck-perl | | Reads YAML files. Fast. |

1. The version in the Postgres APT repo is 3.6.2
1. You may also want libemail-sender-transport-smtps-perl if you want to send e-mail via SSL
1. This has not been released yet. Available from https://github.com/simongreen-net/Email-Stuffer . Earlier versions will work, but won't send HTML messages correctly.

## What the code does

This is rough overview of what the code will do. It only lists the major steps. For example, connecting to the database is not listed.

1. Read the command line options
1. Read the config value
1. Obtain a list of YAML files in the directory
1. For each file
  1. If force-run is specified and job is not this file, skip.
  1. If the last run was a failure and last failure is less than def.delay_after_failure seconds from now, skip.
  1. If the next scheduled run is defined and is later than CURRENT_TIMESTAMP, skip.
  1. Verify that the YAML is correct (e.g. mandatory keys specified, databases exist). If not, e-mail error.
  1. If the status option is specified, get the stats, e-mail them, and then move to the next file.
  1. Run the source SQL.
  1. Run the transform script (if any).
  1. Process the output.
  1. Make changes to the tables in the def schema on the target database.

## Writing transformations

Transformations are Perl subroutines that take information and modify it some way. For example, the `add_totals` transformation will add a totals row to the end of the file for selected columns.

If you want to write your own transformation, simply add a new subroutine in Data::Extract::Transactions. This accepts three parameters:

1. An array of arrays with the data retrieved from the database.
1. An array of headers with the column names.
1. The value from transform » args from the YAML file. The format of this value is up to you.

Note that the first two values are references to an array. If you want to make changes, you must change the values of the references. For example:

    $headers = [qw(new headers)];
    
won't change the headers. Instead you need to write:

    @$headers = qw(new headers);
    
The same applies to the rows arrayref. `splice`, `push`, `pull`, `shift` and `unshift` all will work as expected since they change the original arrayref.


## Considerations

* When writing to a database without transformations and the source database is PostgreSQL, we will use pg_getcopydata and pg_putcopydata. This is much faster than SELECT and INSERT. It also means that we don't need to store the results in memory. For all other jobs, the full results will be stored in memory.
* If the def schema does not exist on the target database, we will automatically create it. If one of the two tables does not exist, they (and their indexes) will also be automatically created.
* The code should be 100% agnostic to the fact that it is written by NZRS, i.e. no references at all to hardcoded location names or NZRS specific methods
* Other than modules from CPAN, the code will have no dependencies on other code written by NZRS.

## What the code doesn't do

* The code is designed to be run only once (without the --force-run or --status flag). Weird things might happen if you run it twice (mainly you might get too copies of a job)
* At the moment, the query takes a single source, and a single target. If multiple sources or targets are required, we can investigate the best option to do so.
* Make the Vodafone Warriors 2017 Telstra Cup Premiers :P


# The Data Extract Facility

The Data Extract Facility (DEF) general function is to process a SQL query, optionally transform the data and the output it. The output could be to a file, an e-mail or another database.

![docs/DEFWorkflow.png?raw=true](Overview of DEF workflow)

## Running the Data Extract Facility

If you are using the DEF for scheduled jobs, it is best to using a tool to schedule regular runnings of the script. This could be done in Jenkins or in a crontab file.

The command is def.pl and the command line options are specified in the section below.

## Command line options

These are the supported command line options for the def.pl script.

| Option | Type | Mandatory | Use |
| --- | --- | --- | --- |
| --config | File location | Yes | Location of the configuration file. |
| --directory | Directory location | No | Location of the directory of YAML file. Required if def.directory is not defined in the config file. |
| --force-run | File location | No | Only process the job in location specified (relative to directory), do not process other rules, and ignore all other jobs |
| --date | Date (YYYY-MM-DD format) | No | If specified will run with place holders based on that date rather than today's date |
| --job-run-id | UUID | No | If specified, will re run the job run with the specified id, --date is ignored. |
| --status | Flag | No | If used, will e-mail stats about jobs that runs to the e-mail address for the job. Does not run the job.
| --verbose | Flag | No | If used, will display status of what it is doing to STDOUT |

## Databases

Any database supported by [DBI](https://metacpan.org/pod/DBI) can be used as a source database. This includes PostgreSQL, MySQL, Oracle and SQLite. However, the target database must be PostgreSQL version 9.2 or above (earlier versions may work but have not been tested)

## Documents

### For end users

* Definition of the [configuration file](docs/ConfigYAML.md).
* Definition of a [job](docs/JobYAML.md).
* A list of supported [transformations](docs/Transformations.md).
* A list of [error codes](docs/ErrorCodes.md).

### For developers

* Details of the [code](docs/Code.md) and required modules.
* POD is also available for all the modules, explaining what each subroutine does.
* The [schema](docs/Schema.md) used for storing job runs.

## Author

Simon Green <simon@nzrs.net.nz>

Copyright 2017 NZRS Ltd

## Licence

This file is part of Data Extract Facility.

The Data Extract Facility is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

The Data Extract Facility is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License along with the Data Extract Facility. If not, see [http://www.gnu.org/licenses/](http://www.gnu.org/licenses/).
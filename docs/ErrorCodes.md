# Error codes

These are the error codes that the Data Extract Facility may produce when running a job, and e-mailing an error. If there is an error running the DEF itself, it will die an return a non zero response to the shell.

| Code | Error |
| --- | --- |
| 100 | An unknown error occurred. |
| 101 | The job YAML file could not be read. |
| 102 | The YAML file was not valid YAML. |
| 103 | A database error occurred. |
| 104 | An error occurred when trying to compress the output. |
| 105 | The requested transformation does not exist. |
| 106 | Could not send e-mail. |
| 107 | Could not write the output to the file system. |
| 108 | Could not connect to the database. |
| 108 | An unknown error occurred. |
| 110 | Could not generate from Template |
| 111 | A command line option was not specified (and no default). |
| 112 | The file or directory does not exist |
| 113 | No jobs found. |
| 120 | A mandatory value in the YAML file was not specified. |
| 121 | The value of the frequency field was not valid. |
| 122 | You specified a transformation, but did state the rule. |
| 123 | The output value was not 'db', 'email', or 'file'. |
| 124 | This type of output must have a filename specified. |
| 125 | Target table not specified. |
| 126 | Target column not specified. |
| 127 | Target table or column is not valid. |
| 128 | Minimum or maximum row count violation. |
| 129 | You specified multiple queries while the output was a database. |
| 130 | A value in the queries element is not an array of hashes. |
| 131 | Unknown file format. |

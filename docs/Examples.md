# Example YAML files

Below are some example YAML files that shows some the functionality of the Data Extract Facility. Detailed documentation of the [configuration YAML](ConfigYAML.md) file and [job YAML](JobYAML.md) file can be found in this directory.

## Example config file

This is an example of a YAML configuration file (that you would pass to the \-\-config option in def.pl):

    def:
      db:
        mydb:
          type: mysql
          host: 127.0.0.1
          database: test
          port: 3306
          is_source: true
        anotherdb:
          host: 127.0.0.1
          database: test
          is_target: true
      mail:
        from: sender@example.org
        host: smtp.example.org
        type: smtp
        ssl: true
      directory: /path/to/directory
      delay_after_failure: 1800
      time_zone: Europe/London

## Example jobs files

### Example 1

This job gets the populations from cities, adds a total, and e-mails the result to `user@example.org` with a subject of "This months population report". This e-mail will have an attachment with the results in CSV format, and the attached file will be in a format like `population-201705.csv`. In addition to the rows retrieved from the database, the attachment will also have a total row, which is provided by the transform rule `add_totals`

Even though this report only sends an e-mail, the `target_db` value is required. This is to track whether or not the job has run on this month. This job will run monthly, on or after the 5th day of the month.

    name: Population totals
    description: A report on the populations of various cities
    frequency: monthly on or after the 5th
    source_db: mydb
    sql: "SELECT name, population FROM cities"
    transform:
      rule: add_totals
      args: [ 2 ]
    output: email
    filename: population-%Y%m.csv
    format: csv
    retry_on_failure: true
    email:
      to: user@example.org
      subject: "This months population report"
    target_db: anotherdb

### Example 2

The following job will get the number of new users that signed up yesterday, and populate the `signup_count` table on the target database. It shows the use of place holders in SQL queries. It will run daily.

    name: New user count
    description: Number of signups yesterday
    frequency: daily
    source_db: mydb
    sql: |
      SELECT '$yesterday$', COUNT(*)
      FROM users
      WHERE created_date >= '$yesterday'
          AND created_date < '$today$'

    output: db
    target_db: anotherdb
    target_table: signups
    target_columns:
      - signup_date
      - user_count

### Example 3

While this job will copy certain values of the user table, after deleting any existing records on the target database (from `target_presql`), and then change the logins to upper case (from `target_postsql`). Once the job has run, an e-mail will be sent to `user@example.org` with the default subject of "NOTIFY: Copy user list".

    name: Copy user list
    description: Copies user list to a different database
    frequency: hourly
    source_db: mydb
    sql: |
      SELECT login_name, real_name, created_date
      FROM users

    output: db
    target_db: anotherdb
    target_table: user_list
    target_columns:
      - login_name
      - real_name
      - created_date
    target_presql: DELETE FROM user_list
    target_postsql: UPDATE user_list SET login = UPPER(login)
    retry_on_failure: false
    notify: true
    email:
      to: user@example.org

package Data::Extract::Job;

use strict;
use warnings;
use 5.10.1;

use Archive::Zip qw':ERROR_CODES :CONSTANTS';
use Data::Dumper;
use Data::Extract::Throwable;
use Data::UUID;
use DateTime;
use DateTime::Format::Pg;
use DateTime::Format::Strptime;
use File::Spec;
use IO::String;
use JSON::XS 'encode_json';
use List::MoreUtils qw'all any mesh';
use POSIX 'fmod';
use Storable 'dclone';
use Text::CSV_XS;
use Time::HiRes 'gettimeofday';
use TryCatch;
use YAML::Syck;

use Data::Extract::Transformations;

sub _format_timestamptz {
    return undef unless defined $_[0];
    DateTime::Format::Pg->format_timestamptz(@_);
}

sub new {
    my $class = shift;
    my $self  = shift;
    bless $self, $class;
    return $self;
}

sub _err {
    my ( $error_code, $error_message ) = @_;

    Data::Extract::Throwable->throw(
        {
            error_code    => $error_code,
            error_message => $error_message
        }
    );
}

sub check_config {
    my ( $self, $config ) = @_;

    $self->runner->debug("Checking YAML configuration");
    foreach my $key (qw(name description frequency source_db)) {
        $config->{$key} || _err( 120, "mandatory field '$key' not specified" );
    }

    unless ( $config->{frequency} eq 'adhoc' ) {
        $config->{target_db}
          || _err( 120, "mandatory field 'target_db' not specified" );

        # Check frequency value
        my $re_tod =
          qr{ after (?:((?:2[0-3]|[01]?[0-9])(?::[0-5]?0-9])?)|(?:(?:1[012]|0?[0-9])(?::[0-5]?0-9])?\s*[ap]m))};
        my $re_dow = qr{ on (?:mon|tues|wednes|thurs|fri|satur|sun)day};
        my $re_dom =
          qr{ on(?: or after)? the (?:[123]?1st|[12]?2nd|[12]?3rd|2?[4-9]th|1[0-9]th|[23]0th)};

        if ( $config->{frequency} !~
            /^(?:adhoc|(?:hourly|daily)$re_tod?|weekly$re_tod?$re_dow?|(?:quarterly|monthly)$re_tod?$re_dow?$re_dom?)$/i
          )
        {
            _err( 121, "Frequency of '$config->{frequency}' not understood" );
        }
    }

    if ( $config->{transform} ) {
        unless ( $config->{transform}{rule} ) {
            _err( 122,
                "If you specify 'transform', you must specify it's rule too" );
        }
    }

    unless ( any { $config->{output} eq $_ } qw(db email file) ) {
        _err( 123, "output must be one of 'db', 'email' or 'file'" );
    }

    unless ( $config->{sql}
        || all { $_->{sql} } @{ $config->{queries} // [ {} ] } )
    {
        _err( 120, "mandatory field 'sql' not specified" );
    }

    unless ( $config->{output} eq 'db'
        || $config->{filename}
        || all { $_->{filename} } @{ $config->{queries} // [ {} ] } )
    {
        _err( 124, "You must specify the filename" );
    }

    if ( $config->{output} eq 'db' ) {
        # Check that the target table name and columns look like valid values
        _err( 125,
            'If the output is db, you must specify the target_table value' )
          unless $config->{target_table};

        _err( 126,
            'If the output is db, you must specify the target_columns value' )
          unless $config->{target_columns};

        _err( 126, 'The target_columns value must be an array' )
          unless ref( $config->{target_columns} ) eq 'ARRAY';

        _err( 127,
                "The target_table value of '"
              . $config->{target_table}
              . "' does not appear valid" )
          unless $config->{target_table} =~ /^[a-z0-9_]+$/i;

        foreach my $col ( @{ $config->{target_columns} } ) {
            _err( 127,
                "The target_columns value of '$col' does not appear valid" )
              unless ref($col) eq '' && $col =~ /^[a-z0-9_]+$/i;
        }

    }

}

sub config {
    my $self = shift;
    state $_config = {};

    unless ( $self->{config_file} ) {
        $self->runner->debug('Reading config file');
        my $directory =
          $self->runner->{directory} || $self->runner->config->{directory};
        my $config_file = File::Spec->catfile( $directory, $self->{file} );
        _err( 101,
            "config file ($config_file) does not exist or is not readable" )
          unless -r $config_file;

        try {
            $_config = LoadFile($config_file);
        }
        catch($e) {
            _err( 102, "Could not read YAML file: $e" );
        };
        $self->{config_file} = $config_file;

        $self->check_config($_config);
    }

    return wantarray ? %$_config : $_config;
}

sub runner {
    return $_[0]->{runner};
}

sub source_db {
    my $self = shift;
    try {
        return $self->runner->get_dbh( $self->config->{source_db}, 'source' );
    }
    catch($e) {
        _err( 108, "Could not connect to the source database: $e" );
    };
}

sub target_db {
    my $self = shift;
    try {
        return $self->runner->get_dbh( $self->config->{target_db}, 'target' );
    }
    catch($e) {
        _err( 108, "Could not connect to the target database: $e" );
    };
}

sub should_run {
    my $self = shift;
    $self->runner->debug('Checking if we should run this job');

    # If the frequency is 'adhoc', do not run. This check is not called
    #  when --force-run is specified
    if ( $self->config->{frequency} eq 'adhoc' ) {
        $self->runner->debug("We don't do checks for adhoc jobs");
        return;
    }

    if ( my $job_run_id = $self->runner->config->{'job-run-id'} ) {

        # Check to see if the job run id (job-run-id) exists for this job
        my $sql = q{
            SELECT j.id, jr.placeholder_date
            FROM jobs j
                JOIN job_run jr ON j.id = jr.job_id
                JOIN job_run_result jrr ON jr.id = jrr.job_run_id
            WHERE j.input_file = ?
              AND jr.id = ?
            ORDER BY jrr.finished DESC
            LIMIT 1
        };

        my ( $job_id, $date ) =
          $self->target_db->selectrow_array( $sql, undef,
            $self->{file}, $job_run_id );
        if ($job_id) {
            $self->runner->debug('The job-run-id exists for this job');
            $self->{jobid}            = $job_id;
            $self->{jobrunid}         = $job_run_id;
            $self->{placeholder_date} = DateTime::Format::Pg->parse_date($date);
            return 1;
        }

        # The jobid does not belong to this job.
        $self->runner->debug('The job-run-id does not exist for this job');
        return;
    }

    # Check to see if the last result was an error, and if so, has the
    #  required elapsed time expired
    my $sql = q{
        SELECT jrr.error, j.id, jr.id, jr.placeholder_date,
            jrr.finished < CURRENT_TIMESTAMP - ?::interval AS can_run
        FROM def.jobs j
            JOIN def.jobs_run jr ON j.id = jr.job_id
            JOIN def.jobs_run_result jrr ON jr.id = jrr.job_run_id
        WHERE j.input_file = ?
            AND j.next_run > CURRENT_TIMESTAMP
        ORDER BY jrr.finished DESC
        LIMIT 1
    };

    my $delay =
      ( $self->runner->config->{delay_after_failure} // 0 ) . ' SECONDS';
    my ( $error, $job_id, $job_run_id, $date, $can_run ) =
      $self->target_db->selectrow_array( $sql, undef, $delay, $self->{file} );
    if ($error) {
        # It's too early to run the job again.
        unless ($can_run) {
            $self->runner->debug(
                "It's too early to run this job after an error");
            return;
        }

        # The last job failed, so we will set the job id manually.
        $self->runner->debug('Running the job after a previous failure');
        $self->{jobid}            = $job_id;
        $self->{jobrunid}         = $job_run_id;
        $self->{placeholder_date} = DateTime::Format::Pg->parse_date($date);
        return 1;
    }

    # Check if the next_run date is in the future
    $sql = q{
        SELECT next_run
        FROM def.jobs
        WHERE next_run > CURRENT_TIMESTAMP
            AND input_file = ?
    };

    my ($next_run) =
      $self->target_db->selectrow_array( $sql, undef, $self->{file} );
    if ($next_run) {
        $self->runner->debug(
            "Job $self->{file} is not due to run until $next_run");
        return;
    }

    $self->runner->debug('A new version of this job is scheduled to run.');
    return 1;
}

sub generate_sql {
    my ( $self, $sql ) = @_;
    return unless $sql;

    if ( $sql =~ /\$[a-z]+\$/ ) {
        my $date = $self->{placeholder_date};

        my %matches = (
            today     => $date->clone,
            yesterday => $date->clone->subtract( days => 1 ),
            thismonth => $date->clone->truncate( to => 'month' ),
            lastmonth =>
              $date->clone->truncate( to => 'month' )->subtract( months => 1 ),
            nextmonth =>
              $date->clone->truncate( to => 'month' )->add( months => 1 ),
            thisfinyear => (
                $date->month < 4 ? DateTime->new(
                    { year => $date->year - 1, month => 4, day => 1 }
                  )
                : DateTime->new(
                    { year => $date->year, month => 4, day => 1 }
                )
            ),
            nextfinyear => (
                $date->month < 4 ? DateTime->new(
                    { year => $date->year, month => 4, day => 1 }
                  )
                : DateTime->new(
                    { year => $date->year + 1, month => 4, day => 1 }
                )
            ),
        );
        while ( my ( $word, $date ) = each %matches ) {
            my $str = $date->ymd();
            $sql =~ s/\$$word\$/$str/g;
        }
    }

    return $sql;
}

sub _generate_filename {
    my $self     = shift;
    my $name     = shift;
    my $compress = shift // 0;

    my $filename = $self->{placeholder_date}->strftime($name);
    $filename .= '.zip'
      if $compress
      && lc( substr( $filename, -4 ) ) ne '.zip';
    $self->runner->debug("The output file name is '$filename'");
    return $filename;
}

sub _check_rows {
    my $self = shift;

    # Check that any minimum or maximums have been met
    my $row_count = $self->{rows};
    if ( my $c = $self->config->{target_min_rows} ) {
        if ( $row_count < $c ) {
            _err( 128,
                "Rows inserted ($row_count) is less than the minimum ($c)" );
        }
    }
    if ( my $c = $self->config->{target_max_rows} ) {
        if ( $row_count > $c ) {
            _err( 128,
                "Rows inserted ($row_count) is less than the minimum ($c)" );
        }
    }
}

sub next_run {
    my $self = shift;
    if ( !$self->{next_run} ) {
        my $freq = $self->config->{frequency};

        # Ad hoc queries never have a next_run day.
        return if $freq eq 'adhoc';

        # Check frequency value
        my $re_tod = qr{ after (\d+):?(\d*)\s*([ap]m)?};
        my $re_dow = qr{ on ((?:mon|tues|wednes|thurs|fri|satur|sun)day)};
        my $re_dom = qr{ on(?: or after)? the (\d+)};

        # Daily => 'day', Weekly => 'week', etc
        my $period =
          index( $freq, ' ' ) == -1
          ? substr( $freq, 0, -2 )
          : substr( $freq, 0, index( $freq, ' ' ) - 2 );
        $period = 'day' if $period eq 'dai';

        my $dt = DateTime->now( time_zone => $self->runner->time_zone );
        if ( $DateTime::VERSION < 1.32 && $period eq 'quarter' ) {
            # Earlier versions don't truncate to quarters
            $dt->truncate( to => 'month' );
            $dt->set_month( int( ( $dt->month - 1 ) / 3 ) * 3 + 1 );
        }
        else {
            $dt->truncate( to => $period );
        }

        # Adjust the time of day if necessary
        if ( my ( $hour, $min, $pm ) = ( $freq =~ $re_tod ) ) {
            $hour += 12 if lc($pm) eq 'pm' && $hour != 12;
            $hour = 0 if lc($pm) eq 'am' && $hour == 12;
            $min = 0 if not defined $min or $min eq '';
            if ( $hour > $dt->hour()
                || ( $hour == $dt->hour() && $min > $dt->minute() ) )
            {
                $dt->set( hour => $hour, minute => $min );
            }
        }

        # Adjust day of month
        if ( my ($dom) = ( $freq =~ $re_dom ) ) {
            $dt->set( day => $dom ) if $dom > $dt->mday();
        }

        # Adjust for day of week
        if ( my ($dow) = ( $freq =~ $re_dow ) ) {
            my $offset = $dt->wday() - 1;    # 0 Mon; 1 Tues ... 6 Sun.
            my %days   = (
                monday    => 0,
                tuesday   => 1,
                wednesday => 2,
                thursday  => 3,
                friday    => 4,
                saturday  => 5,
                sunday    => 6,
            );

            my $add_days = ( $days{ lc $dow } - $offset ) % 7;
            $dt->add( days => $add_days ) if $add_days != 0;
        }

        # If the resulting day or time is in the past, add one $period to the
        #time
        if ( $dt < DateTime->now( time_zone => $self->runner->time_zone ) ) {
            if ( $period eq 'quarter' ) {
                $dt->add( 'months' => 3 );
            }
            else {
                $dt->add( $period . 's' => 1 );
            }
        }

        $self->{next_run} = $dt;
        $self->runner->debug("The next run date is '$dt'");
    }

    return $self->{next_run};
}

sub get_set_job_details {
    my $self = shift;
    $self->{rows}  = 0;
    $self->{error} = '';

    if ( $self->{jobid} && $self->{jobrunid} && $self->{placeholder_date} ) {
        # Set by $self->should_run()
        return;
    }

    $self->{placeholder_date} =
      $self->runner->config->{date}
      ? strptime( '%Y-%m-%d', $self->runner->config->{date} )
      : DateTime->today( time_zone => $self->runner->time_zone );

    # Ad hoc jobs with no target database don't record any information
    return
      if ( $self->config->{frequency} eq 'adhoc'
        && !$self->config->{target_db} );

    if ( $self->target_db->{pg_server_version} >= 90500 ) {
        # Only Postgres 9.5+ have ON CONFLICT
        my $sql = q{
            INSERT INTO def.jobs (input_file, last_run, next_run)
            VALUES ($1, CURRENT_TIMESTAMP, $2)
            ON CONFLICT (input_file)
                DO UPDATE SET last_run = CURRENT_TIMESTAMP, next_run = $2
            RETURNING id
        };

        $self->{jobid} =
          $self->target_db->selectrow_array( $sql, undef, $self->{file},
            _format_timestamptz( $self->next_run ) );
    }
    else {
        # Do it the old fashioned way
        my @sqls = (
            q{
                UPDATE def.jobs SET last_run = CURRENT_TIMESTAMP, next_run = $2
                WHERE input_file = $1
                RETURNING id
            },
            q{
                INSERT INTO def.jobs (input_file, last_run, next_run)
                VALUES ($1, CURRENT_TIMESTAMP, $2)
                RETURNING id
            }
        );
        foreach my $sql (@sqls) {
            $self->{jobid} =
              $self->target_db->selectrow_array( $sql, undef, $self->{file},
                _format_timestamptz( $self->next_run ) );
            last if $self->{jobid};
        }
    }

    my $sql = q{
        INSERT INTO def.jobs_run (id, job_id, placeholder_date)
        VALUES (?, ?, ?)
        RETURNING id
    };

    my $du = Data::UUID->new();

    $self->{jobrunid} =
      $self->target_db->selectrow_array( $sql, undef, $du->create_str(),
        $self->{jobid},
        DateTime::Format::Pg->format_date( $self->{placeholder_date} ) );

    return;
}

sub run_copy {
    my $self = shift;

    my $sql = $self->generate_sql( $self->config->{sql} );
    $self->runner->debug('Running COPY to transfer the data');

    # Since we are using straight copy to copy, we can do this faster by
    #  using Pg's COPY functionality
    $self->source_db->do("COPY ($sql) TO STDOUT");

    $self->target_db->begin_work();
    try {
        # Run the pre-SQL statement (if any)
        if ( $sql = $self->generate_sql( $self->config->{target_presql} ) ) {
            $self->target_db->do($sql);
        }

        my $target_table = $self->config->{target_table};
        my $target_columns =
          join( ', ', @{ $self->config->{target_columns} // [] } );
        $self->target_db->do("COPY $target_table($target_columns) FROM STDIN");

        my $row = undef;
        while ( $self->source_db->pg_getcopydata( \$row ) >= 0 ) {
            $self->target_db->pg_putcopydata($row);
            ++$self->{rows};
        }

        $self->target_db->pg_putcopyend();

        # Check that any minimum or maximums have been met
        $self->_check_rows();

        # Run the post-SQL statement (if any)
        if ( $sql = $self->generate_sql( $self->config->{target_postsql} ) ) {
            $self->target_db->do($sql);
        }
    }
    catch( Data::Extract::Throwable $e) {
        $self->target_db->rollback();
        die($e);
    }
    catch($e) {
        $self->target_db->rollback();
        _err( 103, "An error occurred while copying data: $e" );
    };

    $self->target_db->commit();
    return;
}

sub format_data {
    my ( $self, $format, $rows, $col_names ) = @_;
    $format = lc $format;
    $self->runner->debug("Formating data into $format format");
    my $string = '';

    if ( $format eq 'csv' ) {
        my $obj = IO::String->new($string);
        my $csv = Text::CSV_XS->new( { binary => 1 } );
        $csv->eol("\r\n");
        $csv->print( $obj, $col_names );
        $csv->print( $obj, $_ ) for @$rows;
        $obj->close() or _err( 107, "When generating CSV: $!" );
    }
    else {
        my @output_rows = ();
        foreach my $row (@$rows) {
            push @output_rows, { mesh @$col_names, @$row };
        }
        $string =
          $format eq 'json'
          ? encode_json( \@output_rows )
          : Dump( \@output_rows );
    }

    return $string;
}

sub compress_string {
    my ( $self, $file, $str ) = @_;
    $self->runner->debug("Compressing data");

    my ( undef, undef, $filename ) =
      File::Spec->splitpath( substr( $file, 0, -4 ) );
    my $zip = Archive::Zip->new();
    my $member = $zip->addString( $str, $filename );
    $member->desiredCompressionMethod(COMPRESSION_DEFLATED);
    $member->desiredCompressionLevel(COMPRESSION_LEVEL_BEST_COMPRESSION);

    my $compressed = '';
    my $obj        = IO::String->new($compressed);
    my $status     = $zip->writeToFileHandle( $obj, 1 );
    if ( $status != AZ_OK ) {
        _err( 104, "Compress error" );
    }
    $obj->close();

    return $compressed;
}

sub _write_to_db {
    my $self = shift;
    my $rows = shift;

    $self->target_db->begin_work();
    try {
        if ( my $sql = $self->generate_sql( $self->config->{target_presql} ) ) {
            $self->target_db->do($sql);
        }

        my $target_table = $self->config->{target_table};
        my $target_columns =
          join( ', ', @{ $self->config->{target_columns} // [] } );
        my $values = join ', ',
          ( ('?') x scalar( @{ $self->config->{target_columns} // [] } ) );
        my $csr = $self->target_db->prepare(
            "INSERT INTO $target_table($target_columns) VALUES ($values)");

        foreach my $row (@$rows) {
            $csr->execute(@$row);
        }

        if ( my $sql = $self->generate_sql( $self->config->{target_postsql} ) )
        {
            $self->target_db->do($sql);
        }
    }
    catch( Data::Extract::Throwable $e) {
        $self->target_db->rollback();
        die($e);
    }
    catch($e) {
        $self->target_db->rollback();
        _err( 103, "An error occurred while inserting data: $e" );
    };

    $self->target_db->commit();
    return;
}

sub run_job {
    my $self = shift;

    my $config = dclone( $self->config );

    # If just one query is configured, turn it into a single item array
    unless ( defined $config->{queries} ) {
        $config->{queries} = [ {} ];
        foreach my $field (qw(sql transform filename format compress)) {
            if ( defined $config->{$field} ) {
                $config->{queries}[0]{$field} = delete $config->{$field};
            }
        }
    }
    elsif ( ref( $config->{queries} ) ne 'ARRAY' ) {
        _err( 130, 'The queries element is not an array' );
    }
    else {
        foreach my $i ( 0 .. $#{ $config->{queries} } ) {
            if ( ref( $config->{queries}[$i] ) ne 'HASH' ) {
                ++$i;
                _err( 130, "The $i element queries is not a hash" );
            }
        }
    }

    # You canot have multiple queries if the output is a database
    if ( $config->{output} eq 'db'
        && scalar( @{ $config->{queries} } ) > 1 )
    {
        _err( 129,
            'You cannot specify multiple queries for a database output' );
    }

    my %types = (
        csv  => 'text/csv',
        json => 'application/json',
        yaml => 'text/yaml',
    );

    # Now run each query, and store them in the results array
    $self->{filename} = [];
    my @results = ();
    foreach my $query ( @{ $config->{queries} } ) {
        my $sql = $self->generate_sql( $query->{sql} );

        $self->runner->debug('Getting rows from source database');
        my $csr = $self->source_db->prepare($sql);
        $csr->execute();
        my $col_names = $csr->{NAME_lc};
        my $rows      = $csr->fetchall_arrayref();
        $csr->finish;

        # Do we need to transform the data
        if ( my $trans = $query->{transform} ) {
            if ( my $sub =
                Data::Extract::Transformations->can( $trans->{rule} ) )
            {
                $self->runner->debug('Performing transformation');
                $sub->( $rows, $col_names, $trans->{args} );
            }
            else {
                _err( 105, "Rule '$trans->{rule}' does not exist!" );
            }
        }

        # Record the number of row stored (post-transformation)
        $self->{rows} += scalar(@$rows);

        # If we are writting to a database, we can do that now, and leave
        if ( $config->{output} eq 'db' ) {
            $self->_check_rows();
            $self->runner->debug('Writing output to database');
            $self->_write_to_db($rows);
            return;
        }

        # Transform the data into a string (either YAML, JSON or CSV format)
        my $str = $self->format_data( $query->{format}, $rows, $col_names );

        # Determine the filename and content type
        my $filename =
          $self->_generate_filename( $query->{filename}, $query->{compress} );
        push @{ $self->{filename} }, $filename;

        my $content_type =
          $query->{compress}
          ? 'application/zip'
          : $types{ lc $query->{format} };

        # Do we need to compress the string
        if ( $query->{compress} ) {
            $str = $self->compress_string( $filename, $str );
        }

        push @results,
          {
            data         => $str,
            name         => $filename,
            content_type => $content_type,
          };
    }

    $self->_check_rows();

    if ( $config->{output} eq 'email' ) {
        $self->runner->debug('Sending output via e-mail');

        my $error = $self->runner->send_email(
            {
                from => $config->{email}{from},
                to   => $config->{email}{to},
                subject =>
                  ( $config->{email}{subject} // 'REPORT: ' . $config->{name} ),
                template => 'result',
                params   => {
                    job      => $config,
                    run_time => $self->{started}
                },
                attachments => \@results,
            }
        );

        _err( 106, "Could not send e-mail: $error" ) if $error;
    }
    else {
        $self->runner->debug('Writing output to file');
        foreach my $result (@results) {
            my $file = File::Spec->catfile( $self->runner->{output_directory},
                $result->{name} );
            open( FH, '>', $file )
              or _err( 107, "Cannot write to file ($file): $!" );
            print FH $result->{data};
            close FH;
        }
    }

    return;
}

sub record_job_run {
    my $self = shift;

    # We don't write anything if there is no target_db (e.g. adhoc)
    return unless $self->config->{target_db};

    $self->runner->debug('Recording run result');
    my $sql = q{
        INSERT INTO def.jobs_run_result (job_run_id, started, finished, rows_recorded, error)
        VALUES (?, ?, ?, ?, ?)
    };

    my $error_message = $self->{error} ? $self->{error}->error_message : undef;
    $self->target_db->do(
        $sql,
        undef,
        $self->{jobrunid},
        _format_timestamptz( $self->{started} ),
        _format_timestamptz( $self->{finished} ),
        $self->{rows},
        $error_message,
    );

    if ( $self->{filename} ) {
        $sql = q{UPDATE def.jobs_run SET output_files = ? WHERE id = ?};
        $self->target_db->do( $sql, undef, $self->{filename},
            $self->{jobrunid} );
    }
}

sub notify {
    my $self = shift;
    $self->runner->debug('Sending e-mail notification of run');

    my $params = {
        job      => { $self->config },
        run_time => $self->calculate_diff( $self->{started}, $self->{finished} )
    };

    foreach my $word (qw(placeholder_date jobrunid started finished rows error))
    {
        $params->{$word} = $self->{$word};
    }

    my $error = $self->runner->send_email(
        {
            from    => $self->config->{email}{from},
            to      => $self->config->{email}{to},
            subject => (
                $self->config->{email}{subject}
                  // 'NOTIFY: ' . $self->config->{name}
            ),
            template => 'notify',
            html     => 1,
            params   => $params,
        }
    );

    _err( 106, "Could not send e-mail: $error" ) if $error;
}

sub send_error {
    my $self = shift;
    $self->runner->debug('Sending e-mail notification of error');

    my $params = { job => { $self->config }, };

    foreach my $word (qw(placeholder_date jobrunid started finished rows error))
    {
        $params->{$word} = $self->{$word};
    }

    my $error = $self->runner->send_email(
        {
            from    => $self->config->{email}{from},
            to      => $self->config->{email}{to},
            subject => (
                $self->config->{email}{subject}
                  // 'ERROR: ' . $self->config->{name}
            ),
            template => 'error',
            params   => $params,
        }
    );

    die "Could not send error e-mail: $error" if $error;
}

sub calculate_diff {
    my ( $self, $started, $finished ) = @_;
    # Work out the time it took to run
    my $secs = $finished->hires_epoch() - $started->hires_epoch();
    my @time = ();
    if ( $secs >= 3600 ) {
        push @time, int( $secs / 3600 ) . 'h';
    }
    if ( $secs >= 60 ) {
        push @time, int( $secs % 3600 / 60 ) . 'm';
    }
    push @time, sprintf( '%.03f', fmod( $secs, 60 ) ) . 's';
    return join( ' ', @time );
}

sub generate_stats {
    my $self = shift;
    $self->runner->debug('Generating statistics for job');

    my $sql = q{
        SELECT j.*, jr.cnt_runs, jrf.first_run
        FROM def.jobs j
            LEFT JOIN (
                SELECT job_id, COUNT(*) AS cnt_runs
                FROM def.jobs_run
                GROUP BY job_id
            ) jr ON jr.job_id = j.id
            LEFT JOIN (
                SELECT jr.job_id, MIN(jrr.started) AS first_run
                FROM def.jobs_run jr JOIN def.jobs_run_result jrr
                ON jr.id = jrr.job_run_id
                GROUP BY jr.job_id
            ) jrf ON jrf.job_id = j.id
        WHERE j.input_file = ?
    };

    my $result =
      $self->target_db->selectrow_hashref( $sql, undef, $self->{file} );
    $result->{has_filename} = $result->{has_error} = 0;

    # Get the last ten jobs
    $sql = q{
        SELECT jr.*
        FROM def.jobs_run jr
          JOIN (
              SELECT job_run_id, MIN(started) started
              FROM def.jobs_run_result
              GROUP BY job_run_id
          ) jrr ON jr.id = jrr.job_run_id
        WHERE jr.job_id = ?
        ORDER BY jrr.started DESC
        LIMIT 10
    };

    $result->{runs} =
      $self->target_db->selectall_arrayref( $sql, { Slice => {} },
        $result->{id} );

    $sql = q{
        SELECT *
        FROM def.jobs_run_result
        WHERE job_run_id = ?
        ORDER BY started
    };

    foreach my $run ( @{ $result->{runs} } ) {
        $result->{has_filename} = 1 if $run->{output_files};
        $run->{results} =
          $self->target_db->selectall_arrayref( $sql, { Slice => {} },
            $run->{id} );
        foreach my $result ( @{ $run->{results} } ) {
            $result->{has_error} = 1 if $result->{error};
            $result->{started_dt} =
              DateTime::Format::Pg->parse_timestamptz( $result->{started} );
            $result->{finished_dt} =
              DateTime::Format::Pg->parse_timestamptz( $result->{finished} );
            $result->{run_time} =
              $self->calculate_diff( $result->{started_dt},
                $result->{finished_dt} );

        }
    }

    my $error = $self->runner->send_email(
        {
            from    => $self->config->{email}{from},
            to      => $self->config->{email}{to},
            subject => (
                $self->config->{email}{subject}
                  // 'STATUS: ' . $self->config->{name}
            ),
            template    => 'status',
            html        => 1,
            params      => { job => { $self->config }, status => $result },
            attachments => [
                {
                    name         => $self->{filename},
                    file         => $self->{config_file},
                    content_type => 'text/yaml',
                },
            ],
        }
    );

    _err( 106, "Could not send e-mail: $error" ) if $error;

}

sub should_previous_run {
    my $self = shift;

    # Determine if the run before this one last resulted in an error.

    # Find the most recent job_run_id from the current one
    my $sql = q{
        SELECT o.job_run_id
        FROM (
            SELECT jrr.job_run_id, MIN(jrr.started) first_started
            FROM def.jobs_run jr
                JOIN def.jobs_run_result jrr ON jr.id = jrr.job_run_id
            WHERE jr.job_id = ?
            GROUP BY jrr.job_run_id
        ) o
        WHERE o.first_started <
           (SELECT MIN(started) FROM def.jobs_run_result WHERE job_run_id = ?)
        ORDER BY o.first_started DESC
        LIMIT 1;
    };

    my ($old_job_run_id) =
      $self->target_db->selectrow_array( $sql, undef, $self->{jobid},
        $self->{jobrunid} );
    return unless $old_job_run_id;

    # Check to see if the last result for this run was an error.
    $sql = q{
        SELECT jrr.error, j.id, jr.id, jr.placeholder_date
        FROM def.jobs j
            JOIN def.jobs_run jr ON j.id = jr.job_id
            JOIN def.jobs_run_result jrr ON jr.id = jrr.job_run_id
        WHERE jr.id = ?
        ORDER BY jrr.finished DESC
        LIMIT 1
    };

    my ( $error, $job_id, $job_run_id, $date ) =
      $self->target_db->selectrow_array( $sql, undef, $old_job_run_id );
    if ($error) {

        # The last job failed, so we will create a new run object, and set
        #  some values based on what should_run() would do.
        $self->runner->debug('Running the previouse run for job after failure');
        my $new_self = Data::Extract::Job->new(
            {
                runner           => $self->{runner},
                file             => $self->{file},
                jobid            => $job_id,
                jobrunid         => $job_run_id,
                placeholder_date => DateTime::Format::Pg->parse_date($date),
            }
        );
        $new_self->run();
    }
}

sub run {
    my $self = shift;

    if ( $self->runner->{status} ) {
        $self->generate_stats;
        return 1;
    }

    try {
        $self->{started} = DateTime->from_epoch(
            time_zone => $self->runner->time_zone,
            epoch     => scalar gettimeofday
        );
        $self->get_set_job_details();

        if (   $self->config->{output} eq 'db'
            && !$self->config->{transform}
            && $self->source_db->{Driver}{Name} eq 'Pg' )
        {
            ## TODO: && source db is Postgresql
            $self->run_copy();
        }
        else {
            $self->run_job();
        }

    }
    catch( Data::Extract::Throwable $e) {
        $self->{error} = $e;
    }
    catch($e) {
        $self->{error} = Data::Extract::Throwable->new(
            error_code    => 100,
            error_message => $e
        );
    };

    $self->{finished} = DateTime->from_epoch(
        time_zone => $self->runner->time_zone,
        epoch     => scalar gettimeofday
    );

    # If there is an error, but no jobrunid, we must have died pretty early
    #  in the process (e.g. missing config options). Lets just rethrow the
    #  error
    if ( $self->{error} && !$self->{jobrunid} ) {
        die $self->{error};
    }

    # If this fails, we are in a spot of bother since we won't have recorded
    #  the result. It is unlikely to happen though because we have already
    #  connected to the target database to insert a row in jobs_run (if
    #  applicable)

    $self->record_job_run();

    if ( $self->{error} ) {
        $self->send_error();
    }
    else {
        if ( $self->config->{notify} ) {
            $self->notify();
        }

        if ( $self->config->{'retry-on-failure'} ) {
            $self->should_previous_run();
        }
    }

    return !$self->{error};
}

1;

=head1 NAME

Data::Extract::Job

=head1 DESCRIPTION

This is the module that is used to run a single job in the Data Extract
Facility. It's is generally called from the run subroutine in
Data::Extract::Run.

=head1 METHODS

=head2 _format_timestamptz

=over

=item B<Description>

Converts a DateTime object to a format suitable for PostgreSQL's
"TIMESTAMP WITH TIME ZONE" field. It's a wrapper around the
DateTime::Format::Pg function of the same name to handle the
case when the DateTime object is undef.

=item B<Input>

DateTime object, or undef.

=item B<Returns>

A string, or undef.

=back

=head2 _err

=over

=item B<Description>

Shortcut method to throw an error (Data::Extract::Throwable)

=item B<Input>

=over

=item C<integer> An error number

=item C<string> The error message

=back

=item B<Returns>

Does not return.

=back

=head2 new

=over

=item B<Description>

Constructor method for Data::Extract::Runner

=item B<Input>

A hashref with the following keys:

=over

=item C<file>

C<string> The location of the config file, relative to the directory.
absolute path.

=item C<runner>

A reference to the Data::Extract::Runner object that called this job.

=back

=item B<Returns>

A blessed reference to the object

=back

=head2 check_config

=over

=item B<Description>

Runs checks to see if the YAML file for the job is somewhat valid. This
is NOT an exhaustive check. For example, it does not check that the SQL
is valid.

=item B<Input>

None.

=item B<Returns>

None, will throw an error if there is a problem.

=back

=head2 config

=over

=item B<Description>

Returns information from the configuration file.

=item B<Input>

None.

=item B<Returns>

A hashref that represents the data in the YAML file. The top level 'def' is
removed.

=back

=head2 runner

=over

=item B<Description>

A reference to the runner that called this job

=item B<Input>

None.

=item B<Returns>

A Data::Extract::Runner object.

=back

=head2 source_db

=over

=item B<Description>

Returns a database handle for the source database, making a new handle if
required.

=item B<Input>

The name of the source database.

=item B<Returns>

A database handle to the source database.

=back

=head2 target_db

=over

=item B<Description>

Returns a database handle for the target database, making a new handle if
required. It will also run the required checks to see if the def schema is
set up.

=item B<Input>

The name of the target database.

=item B<Returns>

A database handle to the target database.

=back

=head2 should_run

=over

=item B<Description>

Determines whether this job should be run now. If the last run was an error,
it will see if a certain amount of time as elapsed. If it wasn't an error,
see if the next run value is in the past.

For an adhoc job, will always return false. The runner will run these jobs
if required based on its config options. For all other jobs, will always
return turn.


=item B<Input>

None.

=item B<Returns>

A boolean whether this job should be run or not.

=back

=head2 generate_sql

=over

=item B<Description>

Takes an key from the confiug and generate the SQL that needs to be run.
This will replace relative dates inline, since the COPY statement doesn't
accept placeholders.

=item B<Input>

A key from the configuration to use as a statement. Will return nothing
if this key does not exist, is not defined, or is false.

=item B<Returns>

A SQL statement.

=back

=head2 _generate_filename

=over

=item B<Description>

Returns the name of the output file, given the filename and a compress flag.
The value of the file name is passed through strpftime.

=item B<Input>

Filename and Compress flag

=item B<Returns>

A filename.

=back

=head2 next_run

=over

=item B<Description>

Determines the date that this job should next run. Will always return
undef if the frequency is 'adhoc'

=item B<Input>

None.

=item B<Returns>

A DateTime object.

=back

=head2 get_set_job_details

=over

=item B<Description>

Sets some internal values, and creates a row in the jobs_run table
if required.

=item B<Input>

None.

=item B<Returns>

None.

=back

=head2 run_copy

=over

=item B<Description>

If the source database is PostgreSQL AND there are no transformations AND
the output is a database, then will perform a COPY FROM / COPY TO
database transfer, which is significantly faster than SELECT / INSERT INTO

=item B<Input>

None.

=item B<Returns>

None.

=back

=head2 format_data

=over

=item B<Description>

Turns an array of array into either a JSON, YAML or CSV string

=item B<Input>

=over

=item An array of arrays with the rows of data

=item An array with the column headers

=back

=item B<Returns>

A string, in either JSON, YAML or CSV format

=back

=head2 compress_string

=over

=item B<Description>

Compressed a string with ZIP

=item B<Input>

A string

=item B<Returns>

A string of the input, compressed with zip.

=back

=head2 run_job

=over

=item B<Description>

If the source database is not PostgreSQL OR there are transformations OR the
output is not a database, will use the slower SELECT method to obtain the
data.

=item B<Input>

None.

=item B<Returns>

None. The output will be stored as a file, e-mailed, or stored in a
database as required.

=back

=head2 record_job_run

=over

=item B<Description>

Records the result of run once it has completed. Does nothing if it is an
adhoc job with no target database.

=item B<Input>

None.

=item B<Returns>

None.

=back

=head2 notify

=over

=item B<Description>

If the job has requested to be notified, will send an e-mail on the
completion of a successful run.

=item B<Input>

None.

=item B<Returns>

None.

=back

=head2 send_error

=over

=item B<Description>

If there was an error when running the job, will e-mail the job owner (or
the runner mail.to value if not specified) with a message with the
error code and number.

=item B<Input>

None.

=item B<Returns>

None.

=back

=head2 calculate_diff

=over

=item B<Description>

Calculates the difference in hours, minutes and seconds (with millisecond
precision) between two dates)

=item B<Input>

Two DateTime objects, the first one first.

=item B<Returns>

A string in the form of 'Xh Xm X.XXXs', hours and minutes are not shown if
they are 0.

=back

=head2 generate_stats

=over

=item B<Description>

Gathers statistics about the job and e-mails the owner this information.

=item B<Input>

None.

=item B<Returns>

None.

=back

=head2 run

=over

=item B<Description>

Runs the job.

=item B<Input>

None.

=item B<Returns>

A boolean on whether the job rain successfully or not.

=back

=head1 AUTHOR

Copyright 2017 NZRS Ltd.

=head1 LICENCE

This file is part of Data Extract Facility.

Data Extract Facility is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published by
the Free Software Foundation, either version 3 of the License, or (at your
option) any later version.

Data Extract Facility is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public
License along with Data Extract Facility.  If not, see
<http://www.gnu.org/licenses/>.

=cut

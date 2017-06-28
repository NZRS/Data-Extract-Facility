package Data::Extract::Runner;

use strict;
use warnings;
use 5.10.1;

use Data::Dumper;
use Data::Extract::Job;
use DBI;
use Email::Sender::Transport::Sendmail;
use Email::Sender::Transport::SMTP;
use Email::Stuffer;
use File::Find::Rule;
use FindBin qw($Bin);
use List::MoreUtils 'none';
use Template;
use TryCatch;
use YAML::Syck;

sub new {
    my $class = shift;
    my $self  = shift;
    bless $self, $class;
    return $self;
}

sub config {
    my $self = shift;

    state $config = {};
    unless ( scalar keys %$config ) {
        $self->debug('Loading global config file');
        my $config_file = $self->{config};
        die "--config option not specified\n" unless $config_file;
        die "config file ($config_file) does not exist or is not readable\n"
          unless -r $config_file;

        $config = LoadFile( $self->{config} )->{def};
    }

    return wantarray ? %$config : $config;
}

sub time_zone {
    $_[0]->config->{time_zone} || 'local';
}

sub debug {
    my $self = shift;
    my $msg  = shift;

    say $msg if $self->{verbose};
}

sub send_email {
    # Send an e-mail. Return undef on success, or a text message on error
    my ( $self, $args ) = @_;
    my $to      = $args->{to}      || $self->config->{mail}{to};
    my $from    = $args->{from}    || $self->config->{mail}{from};
    my $subject = $args->{subject} || 'E-mail from the Data Extract Facility';
    my $template    = $args->{template};
    my $params      = $args->{params} // {};
    my $attachments = $args->{attachments} // [];

    return "From address not specified" unless $from;
    return "To address not specified"   unless $to;
    my %email =
      ( to => $to, from => $from, subject => $subject, text_body => '' );

    # Configure transport options
    my %smtp_options = (
        host          => $self->config->{mail}{host},
        ssl           => $self->config->{mail}{ssl},
        port          => $self->config->{mail}{port},
        sasl_username => $self->config->{mail}{username},
        sasl_password => $self->config->{mail}{password},
    );

    foreach my $key ( keys %smtp_options ) {
        # Delete options that are empty
        delete $smtp_options{$key} unless $smtp_options{$key};
    }

    $email{transport} =
      exists( $smtp_options{host} )
      ? Email::Sender::Transport::SMTP->new( \%smtp_options )
      : Email::Sender::Transport::Sendmail->new();

    # Generate the e-mail from the template
    my $tt = Template->new(
        {
            INCLUDE_PATH => "$Bin/../tmpl",
            INTERPOLATE  => 1,
        }
    ) || die "$Template::ERROR\n";

    $params->{config} = $self->config;
    $tt->process( "$template.txt.tmpl", $params, \$email{text_body} )
      || die $tt->error(), "\n";

    if ( $args->{html} ) {
        $email{html_body} = '';
        $tt->process( "$template.html.tmpl", $params, \$email{html_body} )
          || die $tt->error(), "\n";
    }

    # Generate the e-mail
    my $email = Email::Stuffer->new( \%email );

    if ( scalar @$attachments ) {
        foreach my $attach (@$attachments) {
            if ( $attach->{data} ) {
                $email->attach(
                    $attach->{data},
                    filename => ( $attach->{name} // 'unknown' ),
                    content_type =>
                      ( $attach->{content_type} // 'application/octet-stream' ),
                );
            }
            elsif ( $attach->{file} ) {
                $email->attach_file( $attach->{file},
                    content_type => 'text/yaml', );
            }
        }
    }

    # Send it.

    try {
        $self->debug('Sending e-mail');
        $email->send();
    }
    catch( Email::Sender::Failure $e) {
        return $e->message;
    }
    catch($e) {
        return "Unknown error: $e";
    }

    # Return undef on success
    return;
}

sub _check_target_db {
    my ( $self, $dbh ) = @_;

    # We do checks to see if the schema, table and indexes exists
    # CREATE INDEX IF NOT EXISTS only exists in Pg 9.5, and we support 9.2+

    # Create schema if not exists
    my $sql = q{
        SELECT 1
        FROM information_schema.schemata
        WHERE schema_name = ?
    };

    my ($x) = $dbh->selectrow_array( $sql, undef, 'def' );
    unless ($x) {
        $self->debug('Creating schema def');
        $dbh->do('CREATE SCHEMA def');
    }

    my @tables = (
        {
            name    => 'jobs',
            columns => [
                q{id                BIGSERIAL PRIMARY KEY},
                q{input_file        TEXT NOT NULL},
                q{last_run          TIMESTAMP WITH TIME ZONE NOT NULL},
                q{next_run          TIMESTAMP WITH TIME ZONE},
            ],
            indexes => [
                { name => 'jobs_id_idx', unique => 1, column => 'id' },
                {
                    name   => 'jobs_input_file_idx',
                    unique => 1,
                    column => 'input_file'
                },
            ],
        },
        {
            name    => 'jobs_run',
            columns => [
                q{id                UUID NOT NULL PRIMARY KEY},
                q{job_id            BIGINT REFERENCES def.jobs(id) NOT NULL},
                q{placeholder_date  DATE NOT NULL},
                q{output_file       TEXT},
            ],
            indexes => [
                {
                    name   => 'jobs_run_id_idx',
                    unique => 1,
                    column => 'id'
                },
                { name => 'jobs_run_job_id_idx', column => 'job_id' },
            ],
        },
        {
            name    => 'jobs_run_result',
            columns => [
                q{id                BIGSERIAL},
                q{job_run_id        UUID REFERENCES def.jobs_run(id) NOT NULL},
                q{started           TIMESTAMP WITH TIME ZONE NOT NULL},
                q{finished          TIMESTAMP WITH TIME ZONE NOT NULL},
                q{rows_recorded     INTEGER},
                q{error             TEXT},
            ],
            indexes => [
                {
                    name   => 'jobs_run_results_id_idx',
                    unique => 1,
                    column => 'id'
                },
                {
                    name   => 'jobs_run_results_job_run_id_idx',
                    column => 'job_run def.jobs_id'
                },
            ],
        }
    );

    foreach my $table (@tables) {
        my $csr =
          $dbh->statistics_info( undef, 'def', $table->{name}, undef, undef );
        my $table_info = $csr ? $csr->fetchall_arrayref( {} ) : [];

        if ( scalar(@$table_info) == 0 ) {
            # The table does not exist, so create it
            $self->debug( 'Creating table ' . $table->{name} );
            $sql =
                'CREATE TABLE def.'
              . $table->{name} . '('
              . join( ',', @{ $table->{columns} } ) . ')';
            $dbh->do($sql);
        }

        foreach my $index ( @{ $table->{indexes} } ) {
            if (
                none { ( $_->{INDEX_NAME} // '' ) eq $index->{name} }
                @$table_info
              )
            {
                # The index does not exist, so create it
                $self->debug( 'Creating index ' . $index->{name} );
                $sql =
                    'CREATE '
                  . ( $index->{unique} ? 'UNIQUE ' : '' )
                  . 'INDEX '
                  . $index->{name}
                  . ' ON def.'
                  . $table->{name} . ' ('
                  . $index->{column} . ')';
                $dbh->do($sql);
            }
        }
    }
}

sub get_dbh {
    my $self     = shift;
    my $database = shift // '';
    my $type     = shift // '';

    die "Database not specified\n" unless $database;
    die "Type not specified\n"     unless $type;

    state %dbhs;

    unless ( $dbhs{$type}{$database} ) {
        my $details = $self->config->{db}{$database}
          // die "Database '$database' does not exist\n";
        $details->{"is_$type"}
          || die "Database '$database' is not of type '$type'\n";

        my $dbtype = $details->{type} // 'Pg';
        my $conn = join ';',
          (
            map  { $_ . '=' . $details->{$_} }
            grep { !/^(is_|user$|pass$|type$)/ } keys %$details
          );

        my $dbh = $dbhs{$type}{$database} =
          DBI->connect( "dbi:$dbtype:$conn", $details->{user},
            $details->{pass}, { RaiseError => 1, AutoCommit => 1 } )
          || die "Cannot connect to database: $DBI::errstr";

        $self->_check_target_db($dbh) if ( $details->{is_target} );
    }

    return $dbhs{$type}{$database};
}

sub find_jobs {
    my $self      = shift;
    my $directory = $self->{directory};
    die "Directory is not specfied\n" unless $directory;
    die "Directory '$directory' does not exist or is not a directory\n"
      unless -d $directory;

    my @files =
      File::Find::Rule->file()->relative()->name( '*.yml', '*.yaml' )
      ->in($directory);
    my @jobs =
      map { Data::Extract::Job->new( { runner => $self, file => $_ } ) } @files;
    die "No jobs found in the directory '$directory'\n" unless @jobs;
    return wantarray ? @jobs : \@jobs;
}

sub run {
    my $self          = shift;
    my @jobs          = $self->find_jobs;
    my $job_run_count = 0;

    foreach my $job (@jobs) {
        if ( my $force_run = $self->{'force-run'} ) {
            if ( $force_run ne $job->{file} ) {
                $self->debug(
                    "Skipping $job->{file} as it is not the force-run file");
                next;
            }
        }
        elsif ( !$job->should_run() ) {
            $self->debug("Job $job->{file} is not scheduled to run");
            next;
        }

        # Run the job
        $job->run();
        ++$job_run_count;
    }

    return $job_run_count;
}

1;

=head1 NAME

Data::Extract::Runner

=head1 DESCRIPTION

This is the module that is used to run the Data Extract Facility. It's
main responsibility if to run jobs, connect to databases and send e-mails.

=head1 METHODS

=head2 new

=over

=item B<Description>

Constructor method for Data::Extract::Runner

=item B<Input>

A hashref with the following keys:

=over

=item C<config>

C<string> The location of the config file, either relative to CWD, or an
absolute path.

=item C<date>

C<date> The date used for calculating placeholder dates, in YYYY-MM-DD
format. Will default to today if not specified (see time_zone below).

=item C<directory>

C<string> The directory to search for Data Extract Jobs. Files must have the
extension .yml or .yaml to be run.

=item C<force-run>

C<string> Only process the job in location specified (relative to directory),
and ignore all other jobs. The job will run even if it is not yet scheduled to
run again, or it is an adhoc job.

=item C<job-run-id>

C<UUID> If specified, will re run the job run with the specified id, --date is
ignored, since this is derived from the job run placeholder date.

=item C<status>

C<Boolean> Instead of running the job, will e-mail stats about the job to the e-mail
address for the job. Does not run the job.

=item C<verbose>

C<Boolean> If used, will display status of what it is doing to STDOUT.

=back

=item B<Returns>

A blessed reference to the object

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

=head2 time_zone

=over

=item B<Description>

The time zone used by the system when calculating a value for 'today', and
also used when sending out reports.

=item B<Input>

None.

=item B<Returns>

A string representing the time zone that the system uses. This is derived
from the config file, or 'local' if not specified (meaning the local time
zone of the server running the job).

=back

=head2 debug

=over

=item B<Description>

Prints a message to STDOUT if the verbose flag is set.

=item B<Input>

A string that is printed.

=item B<Returns>

None.

=back

=head2 send_email

=over

=item B<Description>

Sends an e-mail with the details specified

=item B<Input>

A hashref with optionally the following keys:

=over

=item C<to>

C<string> or C<array> The e-mail address to send the e-mail to. Will use the
mail.to value from the config if not specified.

=item C<from>

C<string> The e-mail address that will appear in the from address. Can also
be in the format of '"A Person <a.person@email.tld>' too. Will use the
mail.from value from the config if not specified.

=item C<subject>

C<string> The subject of the e-mail message. Will default to 'E-mail from
the Data Extract Facility' if not specified.

=item C<template>

C<string> The file to use as the template, without the extension. The file
should be in a format that Template Toolkit understands.

=item C<params>

C<hash> Paramters to send to the template.

=item C<html>

C<boolean> Whether there is an HTML version of the template that should
also be processed.

=item C<attachments>

C<array of hashes> An array of the attachment(s) to send with the e-mail.
Keys are:

=over

=item C<data>

The content of the attachment

=item C<file>

The file on the local filesystem to send

=item C<name>

The name of the file.

=item C<content_type>

The content-type of the attachment. Default is 'application/octet-stream'

=back

=back

=item B<Returns>

If the mail is sent successfully, will return undef. If there is an error,
it will return the error message.

=back

=head2 _check_target_db

=over

=item B<Description>

Checks to see if the required schema, tables and indexes exist on the
target database, and creates them if they don't.

=item B<Input>

A database handle (a DBI object)

=item B<Returns>

None.

=back

=head2 get_dbh

=over

=item B<Description>

Attempts to connect to the database, and check that the database is of the
correct type (source or target)

=item B<Input>

=over

=item

The name of the database

=item

The type of database ('source' or 'target')

=back

=item B<Returns>

A database handle (a DBI object)

=back

=head2 find_jobs

=over

=item B<Description>

Gets a list of all jobs that exist in the system. This does not do
any filtering based on the frequency or command line options.

=item B<Input>

None

=item B<Returns>

An array or arrayref of Data::Extract::Jobs objects based on files in
the file system that have the extension .yml or .yaml

=back

=head2 run

=over

=item B<Description>

Goes through the list of jobs, and determines if the job shoud run. If that
is true, will run the job.

=item B<Input>

None

=item B<Returns>

The number of jobs actually run.

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

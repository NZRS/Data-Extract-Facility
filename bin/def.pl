#!/usr/bin/perl
$| = 1;

use strict;
use warnings;
use 5.10.1;

use FindBin qw($Bin);
use lib "$Bin/../lib";

use Data::Extract::Runner;
use File::Pid;
use Getopt::Long;
use TryCatch;

my $error = undef;

my %args =
  ( map { $_, '' }
      qw(config date directory help|h force-run job-run-id status verbose) );

GetOptions( \%args,
    qw(config=s date=s directory=s help! force-run=s job-run-id=s status! verbose!)
);

# If we only want the help file, show it and exit.
if ( $args{help} ) {
    system "pod2text", $0;
    exit;
}

my $pidfile = File::Pid->new();
if ( my $num = $pidfile->running ) {
    die "Already running: $num\n";
}
$pidfile->write;

try {
    my $runner = Data::Extract::Runner->new( \%args );
    $runner->run();
}
catch( Data::Extract::Throwable $e) {
    $error = $e;
}
catch($e) {
    $error = Data::Extract::Throwable->new(
        error_code    => 100,
        error_message => $e
    );
};

$pidfile->remove;

if ($error) {
    say 'Error ' . $error->error_code . ' (' . $error->error_message . ')';
    exit $error->error_code;
}

__END__

=head1 NAME

def.pl - Data Extract Facility

=head1 SYNOPSYS

def.pl [options]

=head1 DESCRIPTION

Coomand for the Data Extract Facility.

=head1 OPTIONS

=over 4

=item --config <file>

Location of the configuration file. This value is mandatory.

=item --directory <directory>

Location of the YAML jobs file. If not specified, def.directory will be used
from the config file.

=item --force-run <filename>

Only process the job in location specified (relative to directory), and
ignore all other jobs

=item --date <YYYY-MM-DD>

If specified will run with place holders based on that date rather than
today's date.

=item --help

This help page.

=item --job-run-id <uuid>

If specified, will re run the job run with the specified id, --date is ignored.

=item --status

If used, will e-mail stats about jobs that runs to the e-mail address for the
job. Does not run the job.

=item --verbose

If used, will display status of what it is doing to STDOUT.

=back

=head1 SEE ALSO

More information is available in the README.md file, including links to the
information required in the YAML files.

L<< https://github.com/NZRS/Data-Extract-Facility >>

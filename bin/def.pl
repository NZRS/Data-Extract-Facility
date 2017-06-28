#!/usr/bin/perl

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

my $pidfile = File::Pid->new();
if ( my $num = $pidfile->running ) {
    die "Already running: $num\n";
}
$pidfile->write;

try {
    my %args = ( map { $_, '' }
          qw(config date directory force-run job-run-id status verbose) );

    GetOptions( \%args,
        qw(config=s date=s directory=s force-run=s job-run-id=s status! verbose!)
    );

    my $runner = Data::Extract::Runner->new( \%args );
    $runner->run();
}
catch($e) {
    $error = $e;
};

$pidfile->remove;
die $error if $error;

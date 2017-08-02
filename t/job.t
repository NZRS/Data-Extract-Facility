use Test::More tests => 29;

use DateTime;
use Data::Extract::Job;
use Data::Extract::Runner;
use File::Spec::Functions ':ALL';
use TryCatch;
use YAML::Syck 'Load';

###  _format_timestamptz
is( Data::Extract::Job::_format_timestamptz(undef), undef, 'Value is undef' );
ok( defined( Data::Extract::Job::_format_timestamptz( DateTime->now ) ),
    'Value is a DateTime::Format::Pg' );

###  _err
my $err;
try { Data::Extract::Job::_err( 100, 'Test' ); }
catch($e) { $err = $e; };

isa_ok( $err, 'Data::Extract::Throwable', 'Error object is correct' );
is( $err->error_code,    100,    'Correct error code' );
is( $err->error_message, 'Test', 'Correct error message' );

# Create a runner for the following jobs
my $DIR  = catfile( 't', 'yaml', 'files' );
my $YAML = catfile( 't', 'yaml', 'config.yaml' );
my $runner = Data::Extract::Runner->new(
    { directory => $DIR, config => $YAML, verbose => 1, } );
isa_ok( $runner, 'Data::Extract::Runner', 'Runner object created' );
is( $runner->{config}, $YAML, 'Config is correct' );

###  config
###  check_config

# Check a job with a missing value (e.g. name)
my $job =
  Data::Extract::Job->new( { runner => $runner, file => 'bad-noname.yaml' } );
isa_ok( $job, 'Data::Extract::Job', 'Object is a job' );
try { $job->config->{anything}; }
catch($e) { $err = $e; }

isa_ok( $err, 'Data::Extract::Throwable', 'Error object is correct' );
is( $err->error_code, 120, 'Correct error code' );
is(
    $err->error_message,
    "mandatory field 'name' not specified",
    'Correct error message'
);

# Check a job with a bad frequency
my $err2;
$job = Data::Extract::Job->new(
    { runner => $runner, file => 'bad-frequency.yaml' } );
isa_ok( $job, 'Data::Extract::Job', 'Object is a job' );
try { $job->config->{anything}; }
catch($e) { $err2 = $e; }

isa_ok( $err2, 'Data::Extract::Throwable', 'Error object is correct' );
is( $err2->error_code, 121, 'Correct error code' );
is(
    $err2->error_message,
    "Frequency of 'daily after 13pm' not understood",
    'Correct error message'
);

# Check a good job has no errors
my $err3;
$job =
  Data::Extract::Job->new( { runner => $runner, file => 'good-job.yaml' } );
isa_ok( $job, 'Data::Extract::Job', 'Object is a job' );
try { $job->config->{anything}; }
catch($e) { $err3 = $e; }

ok( !defined($err3), 'No error found' );

###  runner

# Check that we can access the runner. The runner itself has its own tests
isa_ok( $job->runner, 'Data::Extract::Runner', 'Runner object is correct' );

###  source_db
# Cannot test as it requires a database connection

###  target_db
# Cannot test as it requires a database connection

###  should_run
# Cannot test as it requires a database connection

###  get_set_job_details
# This can only run on adhoc jobs without a db connection
$job->get_set_job_details();
is( $job->{rows}, 0, 'rows is set' );
isa_ok( $job->{placeholder_date}, 'DateTime', 'placeholder_date set' );

###  generate_sql
my $today = $job->{placeholder_date}->ymd;
like( $today, qr|^\d{4}\-\d{2}\-\d{2}$|, 'Today looks like a date' );
is(
    $job->generate_sql('sql'),
    qq{SELECT 1, '$today' FROM dual},
    'Correct SQL returned'
);

###  output_filename
is(
    $job->output_filename,
    qq{/tmp/example-$today.yaml.zip},
    'Output file name is correct'
);

###  next_run
# Cannot test as it requires a database connection

###  run_copy
# Cannot test as it requires a database connection

###  format_data

# Source: Australian Bureau of Meteorology 2003
my $headers = [ 'city', 'minimum', 'maximum' ];

my $array = [
    [ 'Sydney',        14.4, 22.3 ],
    [ 'Melbourne',     11.2, 20.1 ],
    [ 'Brisbane',      15.7, 25.3 ],
    [ 'Adelaide',      12.1, 22.1 ],
    [ 'Perth',         12.5, 24.5 ],
    [ 'Hobart',        8.8,  17.2 ],
    [ 'Darwin',        23.4, 32.1 ],
    [ 'Canberra',      6.7,  19.7 ],
    [ 'Alice Springs', 13.2, 28.8 ],
];

my $expected = [
    { city => 'Sydney',        minimum => 14.4, maximum => 22.3 },
    { city => 'Melbourne',     minimum => 11.2, maximum => 20.1 },
    { city => 'Brisbane',      minimum => 15.7, maximum => 25.3 },
    { city => 'Adelaide',      minimum => 12.1, maximum => 22.1 },
    { city => 'Perth',         minimum => 12.5, maximum => 24.5 },
    { city => 'Hobart',        minimum => 8.8,  maximum => 17.2 },
    { city => 'Darwin',        minimum => 23.4, maximum => 32.1 },
    { city => 'Canberra',      minimum => 6.7,  maximum => 19.7 },
    { city => 'Alice Springs', minimum => 13.2, maximum => 28.8 },
];

my $yaml = $job->format_data( $array, $headers );
my $result = Load($yaml);

is_deeply( $result, $expected, 'YAML data formatted as expected' );

###  compress_string

# This test depends on YAML::Syck::Dump and Archive::Zip always producing
#  the same result for the same string.
my $compressed = $job->compress_string($yaml);
is( length($compressed), 330, 'Compressed string is correct size' );

###  run_job
# Cannot test as it requires a database connection

###  record_job_run
# Cannot test as it requires a database connection

###  notify
# Cannot test as it sends e-mail

###  send_error
# Cannot test as it sends e-mail

###  calculate_diff
my $now   = DateTime->now;
my %diffs = (
    3662.5 => '1h 1m 2.500s',
    1081   => '18m 1.000s',
    30.4   => '30.400s',
    0.6    => '0.600s',
);
while ( my ( $diff, $expected ) = each %diffs ) {
    my $start = $now->clone->subtract( nanoseconds => $diff * 1_000_000_000 );
    my $result = $job->calculate_diff( $start, $now );
    is( $result, $expected, "Got the expected result ($expected)" );
}

###  generate_stats
# Cannot test as it requires a database connection

###  should_previous_run
# Cannot test as it requires a database connection

###  run

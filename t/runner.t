use Test::More tests => 3;

use Data::Extract::Runner;
use File::Spec::Functions ':ALL';
use TryCatch;

### new
# Create a runner
my $DIR  = catfile( 't', 'yaml', 'files' );
my $YAML = catfile( 't', 'yaml', 'config.yaml' );
my $runner = Data::Extract::Runner->new(
    { directory => $DIR, config => $YAML, verbose => 1, } );
isa_ok( $runner, 'Data::Extract::Runner', 'Runner object created' );
is( $runner->{config}, $YAML, 'Config is correct' );

### config
# Cannot test as it requires a database connection

### time_zone
is( $runner->time_zone, 'local', 'Correct time zone returned' );

### debug
# Cannot test as it requires a database connection

### send_email
# Cannot test as it sends e-mail

### _check_target_db
# Cannot test as it requires a database connection

### get_dbh
# Cannot test as it requires a database connection

### find_jobs
# Cannot test as it requires a database connection

### run
# Cannot test as it requires a database connection

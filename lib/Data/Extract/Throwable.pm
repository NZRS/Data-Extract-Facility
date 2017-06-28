package Data::Extract::Throwable;

use Moose;
with 'Throwable';

has error_code    => ( is => 'ro' );
has error_message => ( is => 'ro' );

1;

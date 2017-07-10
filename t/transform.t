use Test::More tests => 1;

use Data::Extract::Transformations;

# Source: https://en.wikipedia.org/wiki/List_of_cities_in_Australia_by_population

my @header = ( 'city', 'population' );
my @rows = (
    [ 'Sydney',    5_005_358, ],
    [ 'Melbourne', 4_641_636, ],
    [ 'Brisbane',  2_349_699, ],
    [ 'Perth',     2_066_564, ],
    [ 'Adelaide',  1_326_354, ],
    [ 'Canberra',  396_294, ],
    [ 'Hobart',    222_802, ],
    [ 'Darwin',    143_629, ],
);

my @expected = ( @rows, [ 'TOTAL', 16_152_336 ] );

Data::Extract::Transformations::add_totals( \@rows, \@header, [2] );
is_deeply( \@rows, \@expected, 'Received the expected result' );

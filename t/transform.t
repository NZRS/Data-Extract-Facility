use Test::More tests => 3;

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

my @header = (qw(date fruit cnt));
my @rows   = (
    [ '2017-07-01', 'apple',  10 ],
    [ '2017-07-01', 'banana', 5 ],
    [ '2017-07-01', 'banana', 6 ],
    [ '2017-08-01', 'apple',  12 ],
    [ '2017-08-01', 'carrot', 13 ],
);

my %options = (
    xaxis_column => 2,
    xaxis_value  => 3,
    xaxis_sort   => 'alpha',
    totals       => 'last',
    on_duplicate => 'sum',
    on_empty     => '0',
);

my @expected_header = (qw(date apple banana carrot TOTAL));
my @expected_rows =
  ( [ '2017-07-01', 10, 11, 0, 21 ], [ '2017-08-01', 12, 0, 13, 25 ], );

Data::Extract::Transformations::two_dimensions( \@rows, \@header, \%options );
is_deeply( \@header, \@expected_header, 'Received the expected headers' );
is_deeply( \@rows,    \@expected_rows,    'Received the expected rows' );

package Data::Extract::Transformations;

use strict;
use warnings;
use 5.10.1;

use List::MoreUtils 'firstidx';

sub add_totals {
    my ( $rows, $headers, $config ) = @_;

    die "add_totals was expecting args to be an array"
      unless ref($config) eq 'ARRAY';
    foreach my $col (@$config) {
        die "The value '$col' is not a number" unless $col =~ /[1-9][0-9]*$/;
    }

    my @totals = ( (0) x scalar(@$config) );
    foreach my $row (@$rows) {
        foreach my $cnt ( 0 .. $#$config ) {
            $totals[$cnt] += $row->[ $config->[$cnt] - 1 ];
        }
    }

    my @total_row = ( (undef) x scalar(@$headers) );
    foreach my $cnt ( 0 .. $#$config ) {
        $total_row[ $config->[$cnt] - 1 ] = $totals[$cnt];
    }

    my $cnt = firstidx { !defined($_) } @total_row;
    if ( $cnt != -1 ) {
        $total_row[$cnt] = 'TOTAL';
    }

    push @$rows, \@total_row;
}

1;

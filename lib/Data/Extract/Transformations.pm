package Data::Extract::Transformations;

use strict;
use warnings;
use 5.10.1;

use List::MoreUtils qw'firstidx uniq';
use List::Util 'sum';

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

sub two_dimensions {
    my ( $rows, $headers, $config ) = @_;

    my $columns = scalar(@$headers);
    foreach my $field (qw(xaxis_column xaxis_value)) {
        die "Value for '$field' not specified" unless $config->{$field};
        die "Value for '$field' is not a number"
          unless $config->{$field} =~ /^\d+$/;
        die "Value for '$field' is greater than the number of columns"
          unless $config->{$field} <= $columns;
    }

    # We substract one becuase the column is one less than what is specified
    my $field_column = $config->{xaxis_column} - 1;
    my $value_column = $config->{xaxis_value} - 1;
    my $on_dup       = $config->{on_duplicate} // '';
    my $on_empty     = $config->{on_empty} // '';
    my $x_totals     = $config->{totals} // '';

    # Get a list of unique x values, and sort if required
    my @x_values = uniq map { $_->[$field_column] } @$rows;
    if ( $config->{xaxis_sort} eq 'alpha' ) {
        @x_values = sort @x_values;
    }
    elsif ( $config->{xaxis_sort} eq 'numeric' ) {
        @x_values = sort { $a <=> $b } @x_values;
    }

    # Lets process each input row
    my @results = ();
    foreach my $row (@$rows) {
        # Get the values that are not part of the x-axis
        my @y_axis_values = ();
        for my $col ( 0 .. $columns - 1 ) {
            next if $col == $field_column || $col == $value_column;
            push @y_axis_values, $row->[$col];
        }

        # Do we already have this row?
        my $idx = firstidx {
            my $match = 1;
            foreach my $i ( 0 .. $#y_axis_values ) {
                if ( $y_axis_values[$i] ne $_->{y}[$i] ) { $match = 0; last }
            }
            $match;
        }
        @results;

        if ( $idx == -1 ) {
            # No row found, let's add it. NB: -1 is still valid
            push @results, { y => \@y_axis_values, x => {} };
        }

        # Add or update the x-axis values for this row
        my $x_axis  = $row->[$field_column];
        my $current = $results[$idx]{x}{$x_axis};
        my $value   = $row->[$value_column];

        if ( defined $current ) {
            if ( $on_dup eq 'first' ) { next }
            elsif ( $on_dup eq 'sum' ) { $value += $current }
            elsif ( $on_dup ne 'last' ) {
                die 'Duplicate values found in this row: '
                  . join( ', ', @$row ) . "\n";
            }
            # else latest value is assumed
        }

        $results[$idx]{x}{$x_axis} = $value;
    }

    # Now we have aggregated all the information, we can generate the
    #  resulting rows.
    my @new_rows = ();
    foreach my $row (@results) {
        my @cols = @{ $row->{y} };
        my $total = $x_totals ? sum( values %{ $row->{x} } ) : 0;

        push @cols, $total if $x_totals eq 'first';

        foreach my $x_axis (@x_values) {
            push @cols, $row->{x}{$x_axis} // $on_empty;
        }

        push @cols, $total if $x_totals eq 'last';
        push @new_rows, \@cols;

    }

    # Generate the new header
    my @new_headers = ();
    for my $col ( 0 .. $columns - 1 ) {
        next if $col == $field_column || $col == $value_column;
        push @new_headers, $headers->[$col];
    }
    push @new_headers, 'TOTAL' if $x_totals eq 'first';
    push @new_headers, @x_values;
    push @new_headers, 'TOTAL' if $x_totals eq 'last';

    # And finally, return the new information
    @$headers = @new_headers;
    @$rows    = @new_rows;
}

1;

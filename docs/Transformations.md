# Transformations

The transformation feature allow you to manipulate the data after it is extracted from the source, but before it is written to the destination. Below is a list of the currently supported transformations.

If you want to write your own transformations, please read CODE.md for how to do this.

## add_totals

This transformation will add an extra row to the output with the totals of selected columns. For this transformation the `args` value should be an array of column numbers. In addition to the sums showing, the word 'TOTAL' is added to the first non-blank column (i.e. usually column one unless you are getting the sums of the first column).

For example:

    transform:
        name: add_totals
        args: [2, 4, 6]
        
This would sum the values in the second, forth and sixth columns, and make the first column 'TOTAL'.
 
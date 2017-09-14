# Transformations

The transformation feature allow you to manipulate the data after it is extracted from the source, but before it is written to the destination. Below is a list of the currently supported transformations.

If you want to write your own transformations, please read CODE.md for how to do this.

## add\_totals

This transformation will add an extra row to the output with the totals of selected columns. For this transformation the `args` value should be an array of column numbers. In addition to the sums showing, the word 'TOTAL' is added to the first non-blank column (i.e. usually column one unless you are getting the sums of the first column).

For example:

    transform:
        name: add_totals
        args: [2, 4, 6]

This would sum the values in the second, forth and sixth columns, and make the first column 'TOTAL'.


## two\_dimensions

This transformation take the rows and turn it into a two dimensional array. It takes a number of arguments. The first two are compulsory, the rest are optional.

| Name | Type | Use |
| --- | --- | --- |
| xaxis\_column | Number | The column number (starting from 1) that will be used to generate the header for the x-axis |
| xaxis\_value | Number | The column number (from 1) that will be used as the value |
| xaxis\_sort | 'alpha', 'numeric' | Sort the x-axis headers via alphabetically or numerically. If not specified, the x-axis headers will in the same order as they first appear in the input. |
| totals | 'first', 'last' | Whether to sum up the values for each row. Can be 'first' (will show the before individual value), or 'last' (the last column). Anything else will not create the column. This calculation is done after applying the on_empty value. |
| on\_duplicate | 'first', 'last', 'sum', 'error' |  What to do if a duplicate row is found. First will use the first occurrence of the duplicate, last will use the last occurrence, while 'sum' will add the two values together. The 'error' option will throw an error, which is the default. |
| on\_empty | Text | What to show if a value does not exist. |

### Example

For example, the following data:

    date, fruit, count
    2017-07-01, apple, 10
    2017-07-01, banana, 5
    2017-07-01, banana, 6
    2017-08-01, apple, 12
    2017-08-01, carrot, 13

with the following arguments:

    xaxis_column: 2
    xaxis_value: 3
    xaxis_sort: alpha
    totals: last
    on_duplicate: sum
    on_empty: '0'

will generate the rows:

    date, apple, banana, carrot, TOTAL
    2017-07-01, 10, 11, 0, 21
    2017-08-01, 12, 0, 13, 25


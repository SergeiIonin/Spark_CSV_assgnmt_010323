## Spark Assignment

1) The files include headers, but the column names are arbitrary and vary between files.
2) Both columns contain integer values.
3) The files are a mix of CSV and TSV (tab separated) formats, so your solution needs to accommodate both.
4) Any empty string should be treated as 0.
5) The first column will be referred to as the key, while the second column will be known as the value.
6) Across the entire dataset (all files combined), for each key, there is exactly one value that appears an odd number of times. For example, you might encounter data like this:

Just the value `3` occurs odd number of times (exactly once):
  - 2 : 3
  - 2 : 4
  - 2 : 4

the value `5` occurs odd number of times (three times):
  - 3 : 5
  - 3 : 5
  - 3 : 5

But this combination is prohibited, because three values (`5`, `6` and `7`) occur odd number of times:
  - 3 : 5
  - 4 : 6
  - 5 : 7

In Spark local mode, the application should output file(s) to the specified path in TSV format, with two columns:

- The first column should list each key exactly once.
- The second column should contain the integer that appears an odd number of times for each key, as explained in point 6 above.

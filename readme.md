# csv2sql

Python scripts that convert a csv formatted file or stdin to sqlite database.

# examples

The pcap csv2sql script reads from standard input:

    pcap2csv | python csv2sql

The django csv2sql script reads from a csv file:

    ./csv2sql $csv_file

# installation requirements

-   Python3

# status

Does the job but working towards refactoring each so only a single conversion
script is necessary

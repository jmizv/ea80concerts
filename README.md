# EA80 Concerts
A comprehensive list of EA80 concerts as PostgreSQL table.

The master data for the ~~[German indiepedia page](http://indiepedia.de/index.php?title=EA80-Konzerte)~~, which is unfortunately now defunct.

Check out the complete data as:
* [Markdown document](./blob/master/data/ea80concerts.md)
* [CSV document](./blob/master/data/ea80concerts.csv)
* as aggregated statistics regarding the most [played cities](./blob/master/data/statistics_Ort.md)
* as aggregated statistics regarding the most [played bands](./blob/master/data/statistics_Band.md)

## How it works

The data about the concerts is placed in the CSV file. I've provided a sql script for creating the database.
Use the java file and run it to populate it to the database. After execution the statistic files are generated automatically.
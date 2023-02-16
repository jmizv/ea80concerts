# EA80 Concerts
A comprehensive list of EA80 concerts as PostgreSQL table.

The master data for the ~~[German indiepedia page](http://indiepedia.de/index.php?title=EA80-Konzerte)~~, which is unfortunately now defunct.

Check out the complete data as:
* [Markdown document](./data/ea80concerts.md)
* [CSV document](./data/ea80concerts.csv)
* as aggregated statistics regarding the most [played cities](./data/statistics_Ort.md) (Spoiler: it's Mönchengladbach)
* as aggregated statistics regarding the most [played bands](./data/statistics_Band.md) (Spoiler: it's [Klotzs](https://klotzs.de) <3)

## How it works

The data about the concerts is placed in the CSV file. I've provided a sql script for creating the database.
Use the java file and run it to populate it to the database. After execution the statistic files are generated automatically.

So nothing spectacular. Maybe the code isn't really needed as the statistics are already mind boggling.
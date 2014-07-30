csv2psqldb
==========

Small project to efficiently deploy a .csv file to a PostgreSQL DB.

From the command line:

```
<source path>$: python csv2db <csv file path> <server> <db> <table> <user> <password>
```

*Obs: it takes the table structure from the .csv file; basically meaning that the collumn names `MUST` be present in the first line*

# MyOracle
Simple script to convert MySQL tables to Oracle tables including:
 - Table primary key constraint
 - CRUD stored procedures
 - Table data in csv(ready to be imported on SQLDeveloper)
 - Sequences
 - Triggers
 
all auto generated in just one command only. ;)

## Setup
  
  - Configure database connection on `database.php.example` file.
  - Then rename `database.php.example` to `database.php`.
  - Execute. ;)

## Execute command like this:
```

  php myoracle.php --old_table="<mysql_table_name>" --new_table="<oracle_table_name>" --primary_key="<mysql_table_primary_key>"

```

## Sample Output

![alt text](https://i.imgur.com/c9DONht.png "Output")
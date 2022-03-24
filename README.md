# ==================
# jsharmony-db-iseries
# ==================

jsHarmony Database Connector for DB2/AS400/iSeries via ODBC

## Installation

npm install jsharmony-db-iseries --save

## Usage

```javascript
var JSHiseries = require('jsharmony-db-iseries');
var JSHdb = require('jsharmony-db');
var dbconfig = { _driver: new JSHiseries(), connectionString: "DSN=ODBC;Uid=DBUSER;pwd=DBPASS" };
var db = new JSHdb(dbconfig);
db.Recordset('','select * from C where C_ID >= @C_ID',[JSHdb.types.BigInt],{'C_ID': 10},function(err,rslt){
  console.log(rslt);
  done();
});
```

Note that iseries is by default UPPERCASE; literal selects will need to use explicit aliases to get lowercase result fields `c_id as "c_id"`.

This library uses the [NPM odbc library](https://www.npmjs.com/package/odb).  Use any of the connection settings available in that library.

## Database specific options

### Connection options

```
{
  _driver: ...,
  connectionString: "...",
  options: {
    metadata_filter: [],
    automatic_compound_commands: true,
  }
}
```

#### metadata_filter

```
Default: []   // No metadata will be loaded by default
```

Array of strings to limit database schema introspection on application startup. Currently three formats are supported:

- `"SCHEMA.TABLE"` - Individually specified tables
- `"SCHEMA.%"` - All tables within SCHEMA
- `"%.%"` - All schemas, all tables

#### automatic_compound_commands

```
Default: true
```

If enabled, the driver will automatically wrap `db.Command` statements in a BEGIN...END compound statement to save network roundtrips; otherwise statements must be executed individually.

### Debug Parameters

`config.debug_params.db_perf_reporting = true;`

Enable additional performance logging around the lower level odbc package api calls.

## Missing Features

Database objects (DB.sql.object) interface is not implemented at this time.

## References

[IBM iseries SQL Reference](https://www.ibm.com/docs/en/i/7.1?topic=reference-sql)
[Node ODBC driver](https://www.npmjs.com/package/odbc)

[Article on Encoding (CCSID)](https://developer.ibm.com/articles/dm-0506chong/)
[iseries SQL general overview](http://www.tylogix.com/Articles/iSeries_SQL_Programming_Youve_Got_The_Power.pdf)

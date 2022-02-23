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
db.Recordset('','select * from c where c_id >= @c_id',[JSHdb.types.BigInt],{'c_id': 10},function(err,rslt){
  console.log(rslt);
  done();
});
```

This library uses the NPM odbc library.  Use any of the connection settings available in that library.

## References

https://troels.arvin.dk/db/rdbms/
https://www.ibm.com/docs/en/i/7.2?topic=reference-sql
https://developer.ibm.com/articles/dm-0506chong/
http://www.tylogix.com/Articles/iSeries_SQL_Programming_Youve_Got_The_Power.pdf
https://www.npmjs.com/package/odbc
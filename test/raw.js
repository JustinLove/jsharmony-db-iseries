var odbc = require('odbc');
var async = require('async');
var dbconfig = require('./dbconfig');

var con;

async.waterfall([

  //Connect to DB
  function(cb){
    console.log("attempting connect");
    odbc.connect(dbconfig, function(err, _con){
      if(err) return cb(err);
      con = _con;
      return cb();
    });
  },
  /*
  function(cb){
    console.log("attempting schema");
    con.query("set schema=JSHARMONY1", function(err, rslt){
      if(err) return cb(err);
      var allRows = rslt || [];
      console.log(allRows.length+' found');
      return cb();
    });
  },*/
  /*
  function(cb){
    console.log("attempting global variable");
    con.query("create or replace variable foo int default 0", function(err, rslt){
      if(err) return cb(err);
      var allRows = rslt || [];
      console.log(allRows.length+' found');
      return cb();
    });
  },
  function(cb){
    console.log("attempting minimal statement");
    con.query("select 1 from SYSIBM.SYSDUMMY1", function(err, rslt){
      if(err) return cb(err);
      var allRows = rslt || [];
      console.log(allRows.length+' found');
      return cb();
    });
  },
  function(cb){
    console.log("attempting command statement");
    con.query("set schema=JSHARMONY1; select 1 from SYSIBM.SYSDUMMY1", function(err, rslt){
      if(err) return cb(err);
      var allRows = rslt || [];
      console.log(allRows.length+' found');
      return cb();
    });
  },

  function(cb){
    console.log("attempting multiple statement");
    con.query("begin create or replace variable foo int default 0; select 1 into :FOO from SYSIBM.SYSDUMMY1; end", function(err, rslt){
      if(err) return cb(err);
      var allRows = rslt || [];
      console.log(allRows.length+' found');
      return cb();
    });
  },
  
  function(cb){
    console.log("attempting multiple statement");
    con.query("BEGIN\
      DECLARE BAR INT DEFAULT 1;\
      select FOO into bar from SYSIBM.SYSDUMMY1;\
    END", function(err, rslt){
      if(err) return cb(err);
      var allRows = rslt || [];
      console.log(allRows);
      return cb();
    });
  },
  
  function(cb){
    console.log("attempting version retreival");
    // wrap puts a database version string around it
    con.query("select WRAP('CREATE FUNCTION salary(wage DECFLOAT) RETURNS DECFLOAT RETURN wage * 40 * 52') as x FROM SYSIBM.SYSDUMMY1", function(err, rslt){
      if(err) return cb(err);
      var allRows = rslt || [];
      console.log(allRows[0].X);
      return cb();
    });
  },

  function(cb){
    console.log("attempting drop");
    con.query("BEGIN\
    IF EXISTS (SELECT NAME FROM QSYS2.SYSTABLES WHERE TABLE_SCHEMA = 'JSHARMONY1' AND TABLE_NAME = 'RAWTEST') THEN\
      DROP TABLE JSHARMONY1.RAWTEST;\
    END IF;\
    END", function(err, rslt){
      if(err) return cb(err);
      var allRows = rslt || [];
      console.log(allRows.length+' found');
      return cb();
    });
  },

  function(cb){
    console.log("attempting create");
    con.query("create table rawtest (id numeric(10), name char(50))", function(err, rslt){
      if(err) return cb(err);
      var allRows = rslt || [];
      console.log(allRows.length+' found');
      return cb();
    });
  },

  function(cb){
    console.log("attempting insert");
    con.query("insert into rawtest (id, name) values ( 1, 'name' )", function(err, rslt){
      if(err) return cb(err);
      var allRows = rslt || [];
      console.log(allRows.length+' found');
      return cb();
    });
  },
*/
  //Perform SELECT
  function(cb){
    console.log("attempting select");
    con.query("SELECT id, name FROM jsharmony1.rawtest", function(err, rslt){
      if(err) return cb(err);
      var allRows = rslt || [];
      console.log(allRows.length+' found');
      return cb();
    });
  },
  /*
  //Perform DELETE
  function(cb){
    console.log("attempting delete");
    con.query("delete from rawtest", function(err, rslt){
      if(err) return cb(err);
      return cb();
    });
  },
*/
  /*
  function(cb){
    console.log("attempting if");
    con.query("BEGIN\
    IF 1 = 1\
      THEN SIGNAL SQLSTATE VALUE 'JHERR' SET MESSAGE_TEXT = \'Application Error - Test Error';\
    END IF ;\
  END", function(err, rslt){
      console.log(err, rslt);
      if(err) return cb(err);
      return cb();
    });
  },*/

  //Close Database Connection
  function(cb){
    con.close(function(){ return cb(); });
  },
], function(err){
  if(err) console.log(err);
  console.log('Done');
});
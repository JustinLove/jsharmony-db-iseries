/*
Copyright 2017 apHarmony

This file is part of jsHarmony.

jsHarmony is free software: you can redistribute it and/or modify
it under the terms of the GNU Lesser General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

jsHarmony is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public License
along with this package.  If not, see <http://www.gnu.org/licenses/>.
*/

var DB = require('jsharmony-db');
var types = DB.types;
var typeHandler = require('./DB.iseries.types.js');
var odbc = require('odbc');
var async = require('async');
var _ = require('lodash');
var moment = require('moment');

function DBdriver() {
  this.name = 'iseries';
  this.sql = require('./DB.iseries.sql.js');
  this.pools = []; /* { dbconfig: xxx, con: yyy } */
  this.initStatements = [
    "DECLARE GLOBAL TEMPORARY TABLE SESSION.JSHARMONY_META AS (SELECT 'USystem' CONTEXT FROM SYSIBM.SYSDUMMY1) WITH DATA WITH REPLACE",
  ];

  //Initialize platform
  this.platform = {
    Log: function(msg){ console.log(msg); },
    Config: {
      debug_params: {
        db_log_level: 6,           //Bitmask: 2 = WARNING, 4 = NOTICES :: Database messages logged to the console / log 
        db_error_sql_state: false  //Log SQL state during DB error
      }
    }
  }
  this.platform.Log.info = function(msg){ console.log(msg); }
  this.platform.Log.warning = function(msg){ console.log(msg); }
  this.platform.Log.error = function(msg){ console.log(msg); }
}

DBdriver.prototype.logRawSQL = function(sql){
  if (this.platform.Config.debug_params && this.platform.Config.debug_params.db_raw_sql && this.platform.Log) {
    this.platform.Log.info(sql, { source: 'database_raw_sql' });
  }
}

function initDBConfig(dbconfig){
  if(!dbconfig) return;
  if(!dbconfig.options) dbconfig.options = {};
  if(!dbconfig.options.pooled) dbconfig.options.pooled = false;
}

DBdriver.prototype.getPooledConnection = function (dbconfig, onConnect) {
  if(!dbconfig) throw new Error('dbconfig is required');
  var _this = this;

  var odbcpool = null;
  //Check if pool was already added
  for(var i=0;i<this.pools.length;i++){
    if(this.pools[i].dbconfig==dbconfig) odbcpool = this.pools[i];
  }
  //Add pool if it does not exist
  if(!odbcpool){
    _this.pools.push({
      dbconfig: dbconfig,
      pool: null
    });
    odbcpool = _this.pools[_this.pools.length - 1];
  }
  //Initialize pool connection if it was not initialized
  if(odbcpool.pool){
    onConnect(null, odbcpool.pool);
  } else {
    odbc.pool(dbconfig, function(err, pool) {
      if (!err) odbcpool.pool = pool;
      onConnect(err, pool);
    });
  }
}

DBdriver.prototype.Close = function(onClosed){
  var _this = this;
  async.each(_this.pools, function(odbcpool, pool_cb){
    if(!odbcpool.pool) return pool_cb();
    odbcpool.pool.close(function(){
      odbcpool.pool = null;
      pool_cb();
    });
  }, onClosed);
}

DBdriver.prototype.getDBParam = function (dbtype, val) {
  var _this = this;
  if (!dbtype) throw new Error('Cannot get dbtype of null object');
  if (val === null) return 'NULL';
  if (typeof val === 'undefined') return 'NULL';
  
  if ((dbtype.name == 'VarChar') || (dbtype.name == 'Char')) {
    var valstr = val.toString();
    if ((dbtype.length == types.MAX) || (dbtype.length == -1)) return "'" + _this.escape(valstr) + "'";
    return "'" + _this.escape(valstr.substring(0, dbtype.length)) + "'";
  }
  else if (dbtype.name == 'VarBinary') {
    var valbin = null;
    if (val instanceof Buffer) valbin = val;
    else valbin = Buffer.from(val.toString());
    if (valbin.legth == 0) return "NULL";
    return "BX'" + valbin.toString('hex').toUpperCase() + "'";
  }
  else if ((dbtype.name == 'BigInt') || (dbtype.name == 'Int') || (dbtype.name == 'SmallInt') || (dbtype.name == 'TinyInt')) {
    var valint = parseInt(val);
    if (isNaN(valint)) { return "NULL"; }
    return valint.toString();
  }
  else if (dbtype.name == 'Boolean') {
    if((val==='')||(typeof val == 'undefined')) return "NULL";
    var valbool = val.toString().toUpperCase();
    if(typeHandler.boolParser(val)) return '1';
    return '0';
  }
  else if (dbtype.name == 'Decimal') {
    var valfloat = parseFloat(val);
    if (isNaN(valfloat)) { return "NULL"; }
    return "DECIMAL(" + _this.escape(val.toString()) + ","+dbtype.prec_h+","+dbtype.prec_l+")";
  }
  else if (dbtype.name == 'Float') {
    var valfloat = parseFloat(val);
    if (isNaN(valfloat)) { return "NULL"; }
    return "REAL(" + _this.escape(val.toString()) + ")";
  }
  else if ((dbtype.name == 'Date') || (dbtype.name == 'Time') || (dbtype.name == 'DateTime')) {
    var suffix = '';

    var valdt = null;
    if (val instanceof Date) { valdt = val; }
    else if(_.isNumber(val) && !isNaN(val)){
      valdt = moment(moment.utc(val).format('YYYY-MM-DDTHH:mm:ss.SSS'), "YYYY-MM-DDTHH:mm:ss.SSS").toDate();
    }
    else {
      if (isNaN(Date.parse(val))) return "NULL";
      valdt = new Date(val);
    }

    var mdate = moment(valdt);
    if (!mdate.isValid()) return "NULL";

    if(!_.isNumber(val) && !_.isString(val)){
      if('jsh_utcOffset' in val){
        //Time is in UTC, Offset specifies amount and timezone
        var neg = false;
        if(val.jsh_utcOffset < 0){ neg = true; }
        suffix = moment.utc(new Date(val.jsh_utcOffset*(neg?-1:1)*60*1000)).format('HH:mm');
        //Reverse offset
        suffix = ' '+(neg?'+':'-')+suffix;

        mdate = moment.utc(valdt);
        mdate = mdate.add(val.jsh_utcOffset*-1, 'minutes');
      }

      if('jsh_microseconds' in val){
        var ms_str = "000"+(Math.round(val.jsh_microseconds)).toString();
        ms_str = ms_str.slice(-3);
        suffix = ms_str.replace(/0+$/,'') + suffix;
      }
    }

    var rslt = '';
    if (dbtype.name == 'Date') rslt = "'" + mdate.format('YYYY-MM-DD') + "'";
    else if (dbtype.name == 'Time') rslt = "'" + mdate.format('HH.mm.ss') + suffix + "'";
    else rslt = "'" + mdate.format('YYYY-MM-DD HH:mm:ss.SSS') + suffix + "'";
    return rslt;
  }
  throw new Error('Invalid datetype: ' + JSON.stringify(dbtype));
}

var connectionId = 1;

DBdriver.prototype.ExecSession = function (dbtrans, dbconfig, session, callback) {
  if(!dbconfig) throw new Error('dbconfig is required');
  var _this = this;
  
  if (dbtrans) {
    session(null, dbtrans.con, '', callback);
  }
  else {
    var preStaements = [];
    var onConnect = function (err, con) {
      if (!con.jshId) {
        con.jshId = connectionId++;
        preStatements = _this.initStatements;
        if(dbconfig && dbconfig._presql) preStatements = preStatements.concat(splitSQL(dbconfig._presql));
      }
      console.log('odbc connect', con.jshId);
      if (err) { return _this.ExecError(err, callback, "DB Connect Error: "); }
      session(null, con, preStatements, function () { console.log('odbc close', con.jshId); con.close(); callback.apply(null, arguments); });
    };
    initDBConfig(dbconfig);
    if(dbconfig.options.pooled){  
      var pool = _this.getPooledConnection(dbconfig, function(err, pool) {
        pool.connect(onConnect);
      });
    }
    else {
      odbc.connect(dbconfig, onConnect);
    }
  }
}

/*
https://www.ibm.com/docs/en/i/7.4?topic=reference-sql-messages-codes
https://www.ibm.com/docs/en/i/7.4?topic=application-db2-i-cli-sqlstate-values

    01, is a warning.
    HY, is generated by the CLI driver (either Db2 for i CLI or ODBC).

- If SQLCODE = 0 and SQLWARN0 is blank, execution was successful.
- If SQLCODE = 100, no data was found. For example, a FETCH statement
returned no data, because the cursor was positioned after the last row of the
result table.
- If SQLCODE > 0 and not = 100, execution was successful with a warning.
- If SQLCODE = 0 and SQLWARN0 = 'W', execution was successful with a
warning.
- If SQLCODE < 0, execution was not successful

[Error: [odbc] Error executing the sql statement] {
  odbcErrors: [
    {
      state: 'HY000',
      code: -104,
      message: '[IBM][System i Access ODBC Driver][DB2 for i5/OS]SQL0104 - Token <END-OF-STATEMENT> was not valid. Valid tokens: + - AS <IDENTIFIER>.'
    },
    {
      state: 'HY000',
      code: 69898,
      message: '[IBM][System i Access ODBC Driver][DB2 for i5/OS]PWS0005 - Error occurred in the database host server code.'
    }
  ]
}
*/
DBdriver.prototype.ExecError = function(err, callback, errprefix, sql) {
  if (this.platform.Config.debug_params.db_error_sql_state && !this.silent){
    var errmsg = (errprefix || '');
    if(sql) errmsg += ':: ' + sql + '\n';
    errmsg += err.toString();
    (err.odbcErrors || []).forEach(function(message) {
      if (message.code == 69898) return;
      errmsg += message.message.replace('[IBM][System i Access ODBC Driver][DB2 for i5/OS]', '\n  ');
    });
    this.platform.Log(errmsg, { source: 'database' });
  }
  if (callback) return callback(err, null);
  else throw err;
}

DBdriver.prototype.ExecStatements = function(con, statements, callback) {
  var _this = this;
  async.mapSeries(statements, function(sql, cb) {
    _this.ExecStatement(con, sql, cb);
  }, callback);
}

// response format:
/*
[
  { ONE: 1 },
  statement: 'SELECT 1 AS ONE FROM SYSIBM.SYSDUMMY1',
  parameters: [],
  return: undefined,
  count: 1,
  columns: [
    {
      name: 'ONE',
      dataType: 4,
      columnSize: 10,
      decimalDigits: 0,
      nullable: false
    }
  ]
]
*/
DBdriver.prototype.ExecStatement = function(con, sql, callback) {
  var _this = this;
  _this.logRawSQL(sql);
  con.query(sql, function(err, result) {
    if (err) { return _this.ExecError(err, callback, 'SQL Error: ', sql); }
    callback(err, result);
  });
}

function splitSQL(fsql){
  var sql = [];
  var lastidx=fsql.lastIndexOf('%%%JSEXEC_ESCAPE(');
  //Escape JSEXEC expressions
  while(lastidx >= 0){
    var endPos = fsql.indexOf(')%%%',lastidx);
    if(endPos >= 0){
      var match = fsql.substr(lastidx,endPos-lastidx+4);
      var expr = match.substr(17);
      expr = expr.substr(0,expr.length-4);
      expr = expr.replace(/'/g,"''").replace(/\\;/g,"\\\\\\;").replace(/\r/g," ").replace(/\n/g,"\\n ");
      fsql = fsql.substr(0,lastidx) + expr + fsql.substr(lastidx+match.length);
    }
    if(lastidx == 0) lastidx = -1;
    else lastidx=fsql.lastIndexOf('%%%JSEXEC_ESCAPE(',lastidx-1);
  }
  while(fsql){
    var nexts = fsql.indexOf(';');
    while((nexts > 0) && (fsql[nexts-1]=="\\")) nexts = fsql.indexOf(';', nexts+1);
    if(nexts < 0){ sql.push(fsql.trim()); fsql = ''; }
    else if(nexts==0) fsql = fsql.substr(1);
    else{ sql.push(fsql.substr(0,nexts).trim()); fsql = fsql.substr(nexts+1); }
  }
  for(var i=0;i<sql.length;i++){
    var stmt = sql[i].trim();
    //Remove starting comments
    while((stmt.indexOf('/*')==0)||(stmt.indexOf('//')==0)||(stmt.indexOf('--')==0)){
      if((stmt.indexOf('//')==0)||(stmt.indexOf('--')==0)){
        var eolpos = stmt.indexOf('\n');
        if(eolpos >= 0) stmt = stmt.substr(eolpos+1);
        else stmt = '';
      }
      else if(stmt.indexOf('/*')==0){
        var eoc = stmt.indexOf('*/');
        if(eoc >= 0) stmt = stmt.substr(eoc+2);
        else stmt = '';
      }
      stmt = stmt.trim();
    }
    //Remove empty statements
    var is_empty = stmt.match(/^(\s)*$/);
    var is_comment = stmt.match(/^(\s)*\/\//);
    var is_comment = is_comment || stmt.match(/^(\s)*--/);
    if(is_empty || is_comment){
      sql.splice(i,1);
      i--;
      continue;
    }
    stmt = DB.util.ReplaceAll(stmt, "\\;", ';');
    sql[i] = stmt;
  }
  return sql;
}

DBdriver._splitSQL = splitSQL;

function forEachRecordset(rslt, f){
  for(var i=0;i<rslt.length;i++){
    var rs = rslt[i];
    if((rs.command.toUpperCase()=='SELECT')||(rs.rows && rs.rows.length)){
      var frslt = f(rs);
      if(frslt===false) return;
    }
  }
}

DBdriver.prototype.Exec = function (dbtrans, context, return_type, sql, ptypes, params, callback, dbconfig) {
  if(!dbconfig) throw new Error('dbconfig is required');
  var _this = this;

  var statements = [sql];

  _this.ExecSession(dbtrans, dbconfig, function (err, con, preStatements, conComplete) {
    if(dbtrans && (dbtrans.dbconfig != dbconfig)) err = new Error('Transaction cannot span multiple database connections');
    if(err) return conComplete(err);

    sql = _this.applySQLParams(sql, ptypes, params);

    if (return_type == 'command') {
      sql = DB.util.ReplaceAll(sql, "\\;", ';');
      statements = [ 'BEGIN ' + [].concat(preStatements, _this.getContextStatements(context), sql).join('; ') + (sql.endsWith(';') ? '' : ';') + ' END' ];
    } else {
      statements = splitSQL(sql);
      statements = [].concat(preStatements, _this.getContextStatements(context), statements);
    }

    _this.platform.Log(statements, { source: 'database' });
    //console.log(params);
    //console.log(ptypes);

    //Execute sql
    _this.ExecStatements(con, statements, conComplete);

  }, function(err, rslt) {
    if(err) {
      if (callback != null) callback(err, null);
      else throw err;
      return;
    }

    var dbrslt = null;

    //console.log(return_type, typeof(rslt), rslt);

    var rslt = rslt.filter(function(r) {return r.columns.length > 0});

    //console.log(return_type, typeof(rslt), rslt);

    if (return_type == 'row') { if (rslt[0] && rslt[0].length) dbrslt = rslt[0][0]; }
    else if (return_type == 'recordset') dbrslt = rslt[0];
    else if (return_type == 'multirecordset') dbrslt = rslt;
    else if (return_type == 'scalar') {
      if (rslt[0] && rslt[0].length) {
        var row = rslt[0][0];
        for (var key in row) {
          if (row.hasOwnProperty(key)) dbrslt = row[key];
        }
      }
    }
    var notices = [];
    var warnings = [];
    DB.util.LogDBResult(_this.platform, { sql: sql, dbrslt: dbrslt, notices: notices, warnings: warnings });
    if (callback) callback(null, dbrslt, { notices: notices, warnings: warnings });
  });
};

DBdriver.prototype.escape = function(val){ return this.sql.escape(val); }

DBdriver.prototype.getContextStatements = function(context) {
  if(!context) return [];
  return [
    "UPDATE SESSION.JSHARMONY_META SET CONTEXT = '"+this.escape(context)+"'",
  ];
}

DBdriver.prototype.applySQLParams = function (sql, ptypes, params) {
  var _this = this;

  //Apply ptypes, params to SQL
  var ptypes_ref = {};
  if(ptypes){
    var i = 0;
    for (var p in params) {
      ptypes_ref[p] = ptypes[i];
      i++;
    }
  }
  //Sort params by length
  var param_keys = _.keys(params);
  param_keys.sort(function (a, b) { return b.length - a.length; });
  //Replace params in SQL statement
  for (var i = 0; i < param_keys.length; i++) {
    var p = param_keys[i];
    var val = params[p];
    if (val === '') val = null;
    sql = DB.util.ReplaceAll(sql, '@' + p, _this.getDBParam(ptypes ? ptypes_ref[p] : types.fromValue(val), val));
  }
  return sql;
}

exports = module.exports = DBdriver;

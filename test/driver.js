/*
Copyright 2022 apHarmony

This file is part of jsHarmony.

jsHarmony is free software: you can redistribute it and/or modify
it.skip under the terms of the GNU Lesser General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

jsHarmony is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public License
along with this package.  If not, see <http://www.gnu.org/licenses/>.
*/

var JSHiseries = require('../index');
var JSHdb = require('jsharmony-db');
var types = JSHdb.types;
var assert = require('assert');
var _ = require('lodash');
//var moment = require('moment');
var initialdbconfig = require('./dbconfig');

var driver = new JSHiseries();
driver.platform.Config.debug_params.db_error_sql_state = true;
driver.platform.Config.debug_params.db_raw_sql = true;
//driver.platform.Config.debug_params.db_perf_reporting = true;

var dbconfig = _.extend({_driver: driver, connectionString: "DSN=ODBC;Uid=DBUSER;pwd=DBPASS", initialSize: 1, options: {pooled: true} }, initialdbconfig);

describe('Driver',function(){
  this.timeout(9000);

  after(function(done){
    driver.Close(done);
  });

  it('ExecError', function (done) {
    driver.ExecError({detail: 'detail', position: 5}, function(err, value) {
      assert.ok(err, "callback with err");
      assert.ok(!value, "callback without value");
      done();
    }, "prefix", "some code");
  });

  it('ExecError prefix stripping', function (done) {
    driver.ExecError({
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
    }, function(err, value) {
      console.log(err);
      assert.ok(err.message.match('Token'), "has message");
      assert.ok(!err.message.match('host server code'), "does not have generic error");
      assert.ok(!err.message.match('System i'), "Does not have prefix");
      done();
    }, "prefix", "some code");
  });

  it('logRawSQL', function () {
    var before = driver.platform.Config.debug_params.db_raw_sql;
    driver.platform.Config.debug_params.db_raw_sql = true;
    driver.logRawSQL("some code");
    driver.platform.Config.debug_params.db_raw_sql = before;
  });

  it('ExecSession: unpooled', function(done) {
    var config = _.extend({_driver: driver, connectionString: "DSN=ODBC;Uid=DBUSER;pwd=DBPASS", initialSize: 1 }, initialdbconfig);
    driver.ExecSession(null, config, function(err, con, preStatements, conComplete) {
      assert.ifError(err);
      assert.ok(con, "got a connection");
      assert.ok("caller" in conComplete, "got a callback");
      conComplete(err, 'result');
    }, done);
  });

  it('ExecSession: pooled', function(done) {
    driver.ExecSession(null, dbconfig, function(err, con, preStatements, conComplete) {
      assert.ifError(err);
      assert.ok(con, "got a connection");
      assert.ok("caller" in conComplete, "got a callback");
      conComplete(err, 'result');
    }, done);
  });

  it('escape', function() {
    assert.equal(driver.sql.escape(), '');
    assert.equal(driver.sql.escape(0), 0);
    assert.equal(driver.sql.escape(''), '');
    assert.equal(driver.sql.escape('\t'), '\t');
    assert.equal(driver.sql.escape('\x00'), '');
    assert.equal(driver.sql.escape('don\'t'), 'don\'\'t');
  });

  describe('getDBParam', function() {
    it('NULL', function() {
      assert.equal(driver.getDBParam(types.VarChar(), null), 'NULL');
      assert.equal(driver.getDBParam(types.VarChar(), undefined), 'NULL');
    });
    it('VarChar/Char', function() {
      assert.equal(driver.getDBParam(types.VarChar(10), 'hi'), "'hi'");
      assert.equal(driver.getDBParam(types.VarChar(1), 'hi'), "'h'");
      assert.equal(driver.getDBParam(types.VarChar(types.MAX), 'hi'), "'hi'");
      assert.equal(driver.getDBParam(types.VarChar(), ''), "''");
      assert.equal(driver.getDBParam(types.VarChar(), 'don\'t'), "'don''t'");
      assert.equal(driver.getDBParam(types.Char(10), 'hi'), "'hi'");
    });
    it('VarBinary', function() {
      assert.equal(driver.getDBParam(types.VarBinary(10), 'hi'), "BX'6869'");
      assert.equal(driver.getDBParam(types.VarBinary(1), 'hi'), "BX'6869'"); // ????
      assert.equal(driver.getDBParam(types.VarBinary(types.MAX), 'hi'), "BX'6869'");
      assert.equal(driver.getDBParam(types.VarBinary(), ''), "BX''");
      assert.equal(driver.getDBParam(types.VarBinary(), 'don\'t'), "BX'646F6E2774'");
    });
    it('Ints', function() {
      assert.equal(driver.getDBParam(types.BigInt, 0), "0");
      assert.equal(driver.getDBParam(types.Int, -123456), "-123456");
      assert.equal(driver.getDBParam(types.SmallInt, "123456"), "123456");
      assert.equal(driver.getDBParam(types.TinyInt, 1/0), "NULL");
    });
    it('Bools', function() {
      assert.equal(driver.getDBParam(types.Boolean, true), "1");
      assert.equal(driver.getDBParam(types.Boolean, false), "0");
      assert.equal(driver.getDBParam(types.Boolean, null), "NULL");
    });
    it('Decimal', function() {
      assert.equal(driver.getDBParam(types.Decimal(15, 0), 0), "0");
      assert.equal(driver.getDBParam(types.Decimal(10, 10), 3.14), "3.14");
    });
    it('Float', function() {
      assert.equal(driver.getDBParam(types.Float(15), 0), "0");
      assert.equal(driver.getDBParam(types.Float(10), 3.14), "3.14");
    });
    it('Date', function() {
      assert.equal(driver.getDBParam(types.Date, 0), "DATE('1970-01-01')", 'number');
      assert.equal(driver.getDBParam(types.Date, new Date(2022, 0, 27)), "DATE('2022-01-27')", 'date');
      assert.equal(driver.getDBParam(types.Date, "2022-01-27"), "DATE('2022-01-26')", 'string'); // strings utc
    });
    it('Time', function() {
      assert.equal(driver.getDBParam(types.Time(), 0), "TIME('00.00.00')", 'number');
      assert.equal(driver.getDBParam(types.Time(), new Date(2022, 0, 27, 1, 2, 3)), "TIME('01.02.03')", 'date');
      assert.equal(driver.getDBParam(types.Time(), "2022-01-27 01:02:03"), "TIME('01.02.03')", 'string');
    });
    it('DateTime', function() {
      assert.equal(driver.getDBParam(types.DateTime(), 0), "TIMESTAMP('1970-01-01 00:00:00.000')", 'number');
      assert.equal(driver.getDBParam(types.DateTime(), new Date(2022, 0, 27, 1, 2, 3)), "TIMESTAMP('2022-01-27 01:02:03.000')", 'date');
      assert.equal(driver.getDBParam(types.DateTime(), "2022-01-27 01:02:03"), "TIMESTAMP('2022-01-27 01:02:03.000')", 'string');
    });
  });

  it('applySQLParams', function() {
    assert.equal(driver.applySQLParams("select @one", [types.BigInt], {one: 1}), "select 1");
    assert.equal(driver.applySQLParams("select @one, @two", [types.VarChar(), types.DateTime()], {one: 'one', two: "2022-01-27 01:02:03"}), "select 'one', TIMESTAMP('2022-01-27 01:02:03.000')");
  });

  it('ExecStatement - select', function(done) {
    driver.ExecSession(null, dbconfig, function(err, con, presql, conComplete) {
      assert.ifError(err);
      driver.ExecStatement(con, "SELECT 1 AS ONE FROM SYSIBM.SYSDUMMY1", function(err, result) {
        assert.ifError(err);
        assert(result, 'result is not null');
        assert.equal(result.length, 1);
        assert.equal(result[0].ONE, 1);
        conComplete(err, 'result');
      });
    }, done);
  });

  it('ExecStatement - other', function(done) {
    driver.ExecSession(null, dbconfig, function(err, con, presql, conComplete) {
      assert.ifError(err);
      driver.ExecStatement(con, "DECLARE GLOBAL TEMPORARY TABLE SESSION.JSHARMONY_META AS (SELECT 'USystem' CONTEXT FROM SYSIBM.SYSDUMMY1) WITH DATA WITH REPLACE", function(err, result) {
        assert.ifError(err);
        assert.equal(result, null);
        conComplete(err, 'result');
      });
    }, done);
  });

  it('ExecStatements - select', function(done) {
    driver.ExecSession(null, dbconfig, function(err, con, presql, conComplete) {
      assert.ifError(err);
      driver.ExecStatements(con, ["SELECT 1 AS ONE  FROM SYSIBM.SYSDUMMY1", "SELECT 2 AS TWO  FROM SYSIBM.SYSDUMMY1"], function(err, results) {
        assert.ifError(err);
        assert.equal(results.length, 2);
        assert.equal(results[0][0].ONE, 1);
        assert.equal(results[1][0].TWO, 2);
        conComplete(err, 'result');
      });
    }, done);
  });

  it('ExecStatements - other and select', function(done) {
    driver.ExecSession(null, dbconfig, function(err, con, presql, conComplete) {
      assert.ifError(err);
      driver.ExecStatements(con, ["DECLARE GLOBAL TEMPORARY TABLE SESSION.JSHARMONY_META AS (SELECT 'USystem' CONTEXT FROM SYSIBM.SYSDUMMY1) WITH DATA WITH REPLACE", "SELECT 1 AS ONE  FROM SYSIBM.SYSDUMMY1"], function(err, results) {
        console.log(results);
        assert.ifError(err);
        assert.equal(results.length, 1);
        assert.equal(results[0][0].ONE, 1);
        conComplete(err, 'result');
      });
    }, done);
  });

  describe('getContextStatements', function() {
    it('with context', function() {
      var context = driver.getContextStatements('S1', {});
      assert(context.length > 0);
      assert(_.some(context, function(statement) {return statement.match('S1');}));
    });

    it('without context', function() {
      var context = driver.getContextStatements('', {});
      assert(context.length == 0);
    });

    it('meta once', function() {
      var con = {};
      var context1 = driver.getContextStatements('S1', con);
      var context2 = driver.getContextStatements('S1', con);
      assert.equal(context1.length, 2);
      assert.equal(context2.length, 1);
      assert(_.some(context1, function(statement) {return statement.match('S1');}));
      assert(_.some(context2, function(statement) {return statement.match('S1');}));
    });
  });

  describe('splitSQL', function() {
    it('plain statement', function() {
      var statements = JSHiseries._splitSQL('select 1');
      assert.equal(statements.length, 1);
      assert.equal(statements[0], 'select 1');
    });
    it('trailing semicolon', function() {
      var statements = JSHiseries._splitSQL('select 1;');
      assert.equal(statements.length, 1);
      assert.equal(statements[0], 'select 1');
    });
    it('multiple statements', function() {
      var statements = JSHiseries._splitSQL('select 1; select 2');
      assert.equal(statements.length, 2);
      assert.equal(statements[0], 'select 1');
    });
    it('embedded begin-end', function() {
      var statements = JSHiseries._splitSQL('BEGIN select 1\\; END');
      assert.equal(statements.length, 1);
      assert.equal(statements[0], 'BEGIN select 1; END');
    });
  });

  it('Exec: scalar', function(done) {
    driver.Exec(null, 'S1`', 'scalar', "SELECT 1 AS ONE  FROM SYSIBM.SYSDUMMY1", [], {}, function(err, result, other) {
      assert.ifError(err);
      assert.equal(result, 1);
      done();
    }, dbconfig);
  });

  it('Exec: row', function(done) {
    driver.Exec(null, 'S1`', 'row', "SELECT 1 AS ONE, 2 AS TWO  FROM SYSIBM.SYSDUMMY1", [], {}, function(err, result, other) {
      assert.ifError(err);
      assert.equal(result.ONE, 1);
      assert.equal(result.TWO, 2);
      done();
    }, dbconfig);
  });

  it('Exec: recordset', function(done) {
    driver.Exec(null, 'S1`', 'recordset', "SELECT 1 AS ONE  FROM SYSIBM.SYSDUMMY1", [], {}, function(err, result, other) {
      assert.ifError(err);
      assert.equal(result.length, 1);
      assert.equal(result[0].ONE, 1);
      done();
    }, dbconfig);
  });

  it('Exec: multirecordset', function(done) {
    driver.Exec(null, 'S1`', 'multirecordset', "SELECT 1 AS ONE  FROM SYSIBM.SYSDUMMY1; SELECT 2 AS TWO FROM SYSIBM.SYSDUMMY1", [], {}, function(err, result, other) {
      assert.ifError(err);
      assert.equal(result.length, 2);
      assert.equal(result[0][0].ONE, 1);
      assert.equal(result[1][0].TWO, 2);
      done();
    }, dbconfig);
  });
});
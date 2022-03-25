/*
Copyright 2022 apHarmony

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

var JSHiseries = require('../index');
var JSHdb = require('jsharmony-db');
var assert = require('assert');
var _ = require('lodash');
var moment = require('moment');
var dbconfig = require('./dbconfig');

var tempTable = 'create or replace table temp_c(c_id bigint) on replace delete rows; insert into temp_c(c_id) values (1), (2), (3)';

dbconfig = _.extend({_driver: new JSHiseries(), connectionString: "DSN=ODBC;Uid=DBUSER;pwd=DBPASS", options: {pooled: true, automatic_compound_commands: true} }, dbconfig);
var db = new JSHdb(dbconfig);
dbconfig._driver.platform.Config.debug_params.db_error_sql_state = true;
//dbconfig._driver.platform.Config.debug_params.db_raw_sql = true;

describe('Basic',function(){
  this.timeout(30000);

  before('Create temp table', function (done) {
    //Connect to database and get data
    db.Command('',tempTable,[],{},function(err,rslt){
      assert(!err,'Connect and create temp table returned error');
      return done();
    });
  });
  after(function(done){
    assert(db.dbconfig._driver.pools.length==1,'Pool exists');
    assert(db.dbconfig._driver.pools[0].pool,'Pool connected');
    db.Close(function(){
      assert(!db.dbconfig._driver.pools[0].pool,'Pool closed');
      return done();
    });
  });

  it('Select Parameter', function (done) {
    //Connect to database and get data
    var c_id = '1';
    db.Recordset('','select @c_id "c_id" from sysibm.sysdummy1',[JSHdb.types.BigInt],{'c_id': c_id},function(err,rslt){
      assert(!err,'Success');
      assert(!!rslt, 'result exists');
      assert.equal(rslt.length, 1);
      assert.equal((rslt && rslt.length && rslt[0].c_id), c_id,'Parameter returned correctly');
      return done();
    });
  });
  it('Scalar', function (done) {
    //Connect to database and get data
    db.Scalar('','select count(*) from temp_c',[],{},function(err,rslt){
      assert(!err,'Success');
      assert.equal(rslt, 3, 'Scalar correct');
      return done();
    });
  });
  it('Row', function (done) {
    //Connect to database and get data
    var C_ID = '1';
    db.Row('','select c_id "c_id" from temp_c where c_id=@C_ID;',[JSHdb.types.BigInt],{'C_ID': C_ID},function(err,rslt){
      assert(!err,'Success');
      assert(rslt && (rslt.c_id==C_ID),'Recordset correct');
      return done();
    });
  });
  it('Recordset', function (done) {
    //Connect to database and get data
    db.Recordset('','select c_id "c_id" from temp_c;',[],{},function(err,rslt){
      assert(!err,'Success');
      assert(rslt && rslt.length && (rslt.length==3) && (rslt[0].c_id==1),'Recordset correct');
      return done();
    });
  });
  it('MultiRecordset', function (done) {
    //Connect to database and get data
    db.MultiRecordset('','select c_id "c_id" from temp_c;select count(*) "cnt" from temp_c;',[],{},function(err,rslt){
      assert(!err,'Success');
      assert(rslt && rslt.length && (rslt.length==2),'Multiple recordsets returned');
      assert(rslt[0] && (rslt[0].length==3) && (rslt[0][0].c_id==1),'Recordset 1 correct');
      assert(rslt[1] && (rslt[1].length==1) && (rslt[1][0].cnt==3),'Recordset 2 correct');
      return done();
    });
  });
  it('Error', function (done) {
    //Connect to database and get data
    db.Command('','select b;',[],{},function(err,rslt){
      assert(err,'Success');
      return done();
    });
  });
  it('Getting an id from an insert', function (done) {
    //Connect to database and get data
    // https://www.ibm.com/docs/en/i/7.1?topic=clause-table-reference
    //"If the target of the SQL data change statement is a view that is defined with an INSTEAD OF INSERT trigger, an error is returned."
    db.Scalar('','select c_id as "c_id" from final table (\
                    insert into temp_c(c_id) values (4)\
                  );',[],{},function(err,rslt){
      assert(!err,'Success');
      assert(rslt, 4,'Result correct');
      return done();
    });
  });
  it('Delete Statement', function (done) {
    //Connect to database and get data
    db.Command('','delete from temp_c where c_id=4;',[],{},function(err,rslt){
      assert(!err,'Success');
      return done();
    });
  });
  it('Transact-SQL', function (done) {
    //Connect to database and get data
    db.Scalar('',"BEGIN\
                  declare a bigint default 1\\;\
                    set a = a + 1\\;\
                    set a = a + 1\\;\
                    set a = a + 1\\;\
                    insert into temp_c(c_id) values (a)\\;\
                  end;\
                  select c_id from temp_c order by c_id desc limit 1;\
                  delete from temp_c where c_id=4;",[],{},function(err,rslt){
      assert(!err,'Success');
      assert.equal(rslt, 4,'Result correct');
      return done();
    });
  });
  it('Application Error', function (done) {
    //Connect to database and get data
    db.Scalar('',"BEGIN SIGNAL SQLSTATE VALUE 'JHERR' SET MESSAGE_TEXT = 'Application Error - Test Error'\\; END",[],{},function(err,rslt){
      assert(err,'Exception raised');
      assert.equal(err.message,'Application Error - Test Error','Application Error raised');
      return done();
    });
  });
  it.skip('Application Warning', function (done) {
    //Connect to database and get data
    db.Scalar('',"BEGIN SIGNAL SQLSTATE VALUE '01JHW' SET MESSAGE_TEXT = 'Test Warning'\\; END",[],{},function(err,rslt,stats){
      assert(!err, 'Success');
      assert(stats, 'Stats generated');
      assert(stats.warnings && stats.warnings.length,'Warning generated');
      assert(stats.notices && !stats.notices.length,'No notice generated');
      assert.equal(stats.warnings[0].message, 'Test warning', 'Message correct');
      assert.equal(stats.warnings[0].severity, 'WARNING','Severity valid');
      return done();
    });
  });
  it.skip('Application Notice', function (done) {
    //Connect to database and get data
    db.Scalar('',"BEGIN SIGNAL SQLSTATE VALUE '01JHN' SET MESSAGE_TEXT = 'Test Notice'\\; END",[],{},function(err,rslt,stats){
      assert(!err, 'Success');
      assert(stats.notices && stats.notices.length,'Notice generated');
      assert(stats.notices && !stats.warnings.length,'No warnings generated');
      assert((stats.notices[0].message=='Test notice') && (stats.notices[0].severity=='NOTICE'),'Notice valid');
      return done();
    });
  });
  it('Context', function (done) {
    //Connect to database and get data
    db.Scalar('CONTEXT',"select context from session.jsharmony_meta",[],{},function(err,rslt){
      assert(rslt && (rslt.toString().substr(0,7)=='CONTEXT'),'Context found');
      return done();
    });
  });
  it.skip('Bad Transaction', function (done) {
    //Connect to database and get data
    db.ExecTransTasks({
      task1: function(dbtrans, callback, transtbl){
        db.Command('','insert into temp_c(c_id) values(4);',[],{},dbtrans,function(err,rslt){ callback(err, rslt); });
      },
      task2: function(dbtrans, callback, transtbl){
        db.Recordset('','select * from temp_c',[],{},dbtrans,function(err,rslt){ assert(rslt && (rslt.length==4),'Row count correct'); callback(err, rslt); });
      },
      task3: function(dbtrans, callback, transtbl){
        db.Recordset('',"BEGIN SIGNAL SQLSTATE VALUE 'JHERR' SET MESSAGE_TEXT = 'Application Error - Test Error'\\; END",[],{},dbtrans,function(err,rslt){ callback(err, rslt); });
      },
    },function(err,rslt){
      assert(err,'Rollback generated an error');
      assert.equal(err.message, 'Application Error - Test Error','Application Error raised');
      return done();
    });
  });
  it.skip('Transaction Rolled back', function (done) {
    //Connect to database and get data
    db.Scalar('','select count(*) from temp_c',[],{},function(err,rslt){
      assert(!err,'Success');
      assert.equal(rslt,3,'Row count correct');
      return done();
    });
  });

  it.skip('Good Transaction', function (done) {
    //Connect to database and get data
    db.ExecTransTasks({
      task1: function(dbtrans, callback, transtbl){
        db.Command('','insert into temp_c(c_id) values(4);',[],{},dbtrans,function(err,rslt,stats){ callback(err, rslt, stats); });
      },
      task2: function(dbtrans, callback, transtbl){
        db.Command('','insert into temp_c(c_id) values(5);',[],{},dbtrans,function(err,rslt,stats){ callback(err, rslt, stats); });
      },
      task3: function(dbtrans, callback, transtbl){
        db.Command('',"BEGIN SIGNAL SQLSTATE VALUE '01JHW' SET MESSAGE_TEXT = 'Test Warning'\\; END",[],{},dbtrans,function(err,rslt,stats){ callback(err, rslt, stats); });
      },
      task4: function(dbtrans, callback, transtbl){
        db.Command('',"BEGIN SIGNAL SQLSTATE VALUE '01JHN' SET MESSAGE_TEXT = 'Test Notice'\\; END",[],{},dbtrans,function(err,rslt,stats){ callback(err, rslt, stats); });
      },
      task5: function(dbtrans, callback, transtbl){
        db.Recordset('',"select count(*) count from temp_c",[],{},dbtrans,function(err,rslt,stats){ callback(err, rslt, stats); });
      },
    },function(err,rslt,stats){
      assert(!err,'Success');
      assert.equal(rslt.task5.length, 1,'task5 result');
      assert.equal((rslt.task5.length==1)&&(rslt.task5[0].count),5,'Correct result');
      //assert((stats.task3.warnings[0].message=='Test warning'),'Warning generated');
      //assert((stats.task4.notices[0].message=='Test notice'),'Notice generated');
      return done();
    });
  });
  it.skip('Transaction Committed', function (done) {
    //Connect to database and get data
    db.Scalar('','select count(*) from temp_c',[],{},function(err,rslt){
      assert(!err,'Success');
      assert(rslt==5,'Row count correct');
      return done();
    });
  });
  it('Drop temp table', function (done) {
    //Connect to database and get data
    db.Scalar('','drop table temp_c;',[],{},function(err,rslt){
      assert(!err,'Success');
      return done();
    });
  });
  it('ExecTasks - One item', function (done) {
    //Connect to database and get data
    db.ExecTasks([
      function(callback){
        db.Recordset('','select 1 "a" from SYSIBM.SYSDUMMY1',[],{},undefined,function(err,rslt,stats){ callback(err, rslt, stats); });
      }
    ],function(err,rslt,stats){
      assert(!err,'Success');
      assert(rslt,'Produced result');
      assert.equal(rslt&&rslt.length,1,'One Result');
      assert.equal(rslt&&rslt.length&&rslt[0].length, 1,'One Row');
      assert.equal(rslt&&rslt.length&&rslt[0].length&&rslt[0][0].a,1,'Correct result');
      return done();
    });
  });
  it('ExecTasks - Parallel', function (done) {
    //Connect to database and get data
    db.ExecTasks({
      task1: function(callback){
        db.Recordset('','select 1 "a" from SYSIBM.SYSDUMMY1',[],{},undefined,function(err,rslt,stats){ callback(err, rslt, stats); });
      },
      task2: function(callback){
        db.Recordset('','select 2 "b" from SYSIBM.SYSDUMMY1',[],{},undefined,function(err,rslt,stats){ callback(err, rslt, stats); });
      },
      task3: function(callback){
        db.Command('',"BEGIN SIGNAL SQLSTATE VALUE '01JHW' SET MESSAGE_TEXT = 'Test Warning'\\; END",[],{},undefined,function(err,rslt,stats){ callback(err, rslt, stats); });
      },
      task4: function(callback){
        db.Command('',"BEGIN SIGNAL SQLSTATE VALUE '01JHN' SET MESSAGE_TEXT = 'Test Notice'\\; END",[],{},undefined,function(err,rslt,stats){ callback(err, rslt, stats); });
      },
    },function(err,rslt,stats){
      assert(!err,'Success');
      assert.equal(rslt.task1.length,1,'One Result');
      assert.equal(rslt.task1.length&&rslt.task1[0].a,1,'Correct result');
      //assert((stats.task3.warnings[0].message=='Test warning'),'Warning generated');
      //assert((stats.task4.notices[0].message=='Test notice'),'Notice generated');
      return done();
    });
  });
  it('ExecTasks - Serial & Parallel', function (done) {
    //Connect to database and get data
    var dbtasks = [{}, {}];
    dbtasks[0] = {
      task11: function(callback){
        db.Recordset('','select 1 "a" from SYSIBM.SYSDUMMY1',[],{},undefined,function(err,rslt,stats){ callback(err, rslt, stats); });
      },
      task12: function(callback){
        db.Recordset('','select 2 "b" from SYSIBM.SYSDUMMY1',[],{},undefined,function(err,rslt,stats){ callback(err, rslt, stats); });
      },
      task13: function(callback){
        db.Command('',"BEGIN SIGNAL SQLSTATE VALUE '01JHW' SET MESSAGE_TEXT = 'Test Warning'\\; END",[],{},undefined,function(err,rslt,stats){ callback(err, rslt, stats); });
      },
      task14: function(callback){
        db.Command('',"BEGIN SIGNAL SQLSTATE VALUE '01JHN' SET MESSAGE_TEXT = 'Test Notice'\\; END",[],{},undefined,function(err,rslt,stats){ callback(err, rslt, stats); });
      },
    };
    dbtasks[1] = {
      task21: function(callback,dbrslt){
        assert(dbrslt.task11 && dbrslt.task11[0] && (dbrslt.task11[0].a==1),'Series execution worked');
        db.Recordset('','select 1 "a" from SYSIBM.SYSDUMMY1',[],{},undefined,function(err,rslt,stats){ callback(err, rslt, stats); });
      },
      task22: function(callback){
        db.Recordset('','select 2 "b" from SYSIBM.SYSDUMMY1',[],{},undefined,function(err,rslt,stats){ callback(err, rslt, stats); });
      },
      task23: function(callback){
        db.Command('',"BEGIN SIGNAL SQLSTATE VALUE '01JHW' SET MESSAGE_TEXT = 'Test Warning2'\\; END",[],{},undefined,function(err,rslt,stats){ callback(err, rslt, stats); });
      },
      task24: function(callback){
        db.Command('',"BEGIN SIGNAL SQLSTATE VALUE '01JHN' SET MESSAGE_TEXT = 'Test Notice2'\\; END",[],{},undefined,function(err,rslt,stats){ callback(err, rslt, stats); });
      },
    };
    db.ExecTasks(dbtasks, function(err,rslt,stats){
      assert(!err,'Success');
      assert((rslt.task11.length==1)&&(rslt.task11[0].a==1),'Correct result');
      assert((rslt.task21.length==1)&&(rslt.task21[0].a==1),'Correct result');
      //assert((stats.task13.warnings[0].message=='Test warning'),'Warning generated');
      //assert((stats.task14.notices[0].message=='Test notice'),'Notice generated');
      //assert((stats.task23.warnings[0].message=='Test warning2'),'Warning2 generated');
      //assert((stats.task24.notices[0].message=='Test notice2'),'Notice2 generated');
      return done();
    });
  });
  it('ExecTasks - Serial & Parallel Array', function (done) {
    //Connect to database and get data
    var dbtasks = [{}, {}];
    dbtasks[0] = [
      function(callback){
        db.Recordset('','select 1 "a" from SYSIBM.SYSDUMMY1',[],{},undefined,function(err,rslt,stats){ callback(err, rslt, stats); });
      },
      function(callback){
        db.Recordset('','select 2 "b" from SYSIBM.SYSDUMMY1',[],{},undefined,function(err,rslt,stats){ callback(err, rslt, stats); });
      },
      function(callback){
        db.Command('',"BEGIN SIGNAL SQLSTATE VALUE '01JHW' SET MESSAGE_TEXT = 'Test Warning'\\; END",[],{},undefined,function(err,rslt,stats){ callback(err, rslt, stats); });
      },
      function(callback){
        db.Command('',"BEGIN SIGNAL SQLSTATE VALUE '01JHN' SET MESSAGE_TEXT = 'Test Notice'\\; END",[],{},undefined,function(err,rslt,stats){ callback(err, rslt, stats); });
      },
    ];
    dbtasks[1] = [
      function(callback,dbrslt){
        assert(dbrslt[0] && dbrslt[0][0] && (dbrslt[0][0].a==1),'Series execution worked');
        db.Recordset('','select 1 "a" from SYSIBM.SYSDUMMY1',[],{},undefined,function(err,rslt,stats){ callback(err, rslt, stats); });
      },
      function(callback){
        db.Recordset('','select 2 "b" from SYSIBM.SYSDUMMY1',[],{},undefined,function(err,rslt,stats){ callback(err, rslt, stats); });
      },
      function(callback){
        db.Command('',"BEGIN SIGNAL SQLSTATE VALUE '01JHW' SET MESSAGE_TEXT = 'Test Warning2'\\; END",[],{},undefined,function(err,rslt,stats){ callback(err, rslt, stats); });
      },
      function(callback){
        db.Command('',"BEGIN SIGNAL SQLSTATE VALUE '01JHN' SET MESSAGE_TEXT = 'Test Notice2'\\; END",[],{},undefined,function(err,rslt,stats){ callback(err, rslt, stats); });
      },
    ];
    db.ExecTasks(dbtasks, function(err,rslt,stats){
      assert(!err,'Success');
      assert((rslt[0].length==1)&&(rslt[0][0].a==1),'Correct result');
      assert((rslt[4].length==1)&&(rslt[4][0].a==1),'Correct result');
      //assert((stats[2].warnings[0].message=='Test warning'),'Warning generated');
      //assert((stats[3].notices[0].message=='Test notice'),'Notice generated');
      //assert((stats[6].warnings[0].message=='Test warning2'),'Warning2 generated');
      //assert((stats[7].notices[0].message=='Test notice2'),'Notice2 generated');
      return done();
    });
  });
  it('DB Script Notices', function (done) {
    db.SQLExt.Scripts['test'] = {};
    db.SQLExt.Scripts['test']['dropfakedb'] = ["drop database if exists fakedbthatdoesnotexist"];
    db.RunScripts(db.platform, ['test','dropfakedb'],{},function(err,rslt,stats){
      assert(!err,'Success');
      //assert(stats[0].notices[0].message.indexOf('skipping')>=0,'Script generated notice that we are skipping the drop');
      return done();
    });
  });
  it('Date passthru', function (done) {
    //Connect to database and get data
    db.Scalar('',"select char(@dt, USA) from SYSIBM.SYSDUMMY1",[JSHdb.types.Date],{'dt': moment('2018-12-03').toDate()},function(err,rslt){
      assert(!err,'Success');
      assert.equal(rslt,'12/03/2018','Date passthru');
      return done();
    });
  });
  it('DateTime passthru', function (done) {
    //Connect to database and get data
    db.Scalar('',"select char(date(@dt), USA) from SYSIBM.SYSDUMMY1",[JSHdb.types.DateTime(7)],{'dt': moment('2018-12-03').toDate()},function(err,rslt){
      assert(!err,'Success');
      assert.equal(rslt,'12/03/2018','DateTime passthru');
      return done();
    });
  });
});
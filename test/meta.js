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
var assert = require('assert');
var _ = require('lodash');
var initialdbconfig = require('./dbconfig');

var driver = new JSHiseries();

var dbconfig = _.extend({_driver: driver, connectionString: "DSN=ODBC;Uid=DBUSER;pwd=DBPASS", initialSize: 1, options: {pooled: true, metadata_filter: ['JSHARMONY1.%']} }, initialdbconfig);

var db = new JSHdb(dbconfig);
driver.platform.Config.debug_params.db_error_sql_state = true;
driver.platform.Config.debug_params.db_raw_sql = true;

describe('Meta',function(){
  this.timeout(9000);

  after(function(done){
    driver.Close(done);
  });

  it('metaInclude - filtered', function() {
    assert.equal(db.meta.metaInclude('TABLE_SCHEMA', 'TABLE_NAME', ['jsharmony1.%', 'jsharmony1.alltypes']), "((TABLE_SCHEMA = 'JSHARMONY1' AND TABLE_NAME = 'ALLTYPES') OR TABLE_SCHEMA IN ('JSHARMONY1'))");
  });

  it('metaInclude - all', function() {
    assert.equal(db.meta.metaInclude('TABLE_SCHEMA', 'TABLE_NAME', ['%.%']), "(TABLE_SCHEMA NOT LIKE 'Q%' AND TABLE_NAME NOT IN ('SYSIBM', 'SYSIBMADM', 'SYSTOOLS'))");
  });

  it('metaInclude - empty', function() {
    assert.equal(db.meta.metaInclude('TABLE_SCHEMA', 'TABLE_NAME', []), "(0=1)");
  });

  it('metaIncludejoin - filtered', function() {
    assert.equal(db.meta.metaIncludeJoin('T.TABLE_SCHEMA', 'T.TABLE_NAME', ['jsharmony1.%', 'jsharmony1.alltypes']), "");
  });

  it('metaIncludejoin - blank', function() {
    assert.equal(db.meta.metaIncludeJoin('T.TABLE_SCHEMA', 'T.TABLE_NAME', null), " INNER JOIN QSYS2.TABLES I ON (T.TABLE_SCHEMA = I.TABLE_SCHEMA AND T.TABLE_NAME = I.TABLE_NAME) ");
  });

  it('metaIncludejoin - empty', function() {
    assert.equal(db.meta.metaIncludeJoin('T.TABLE_SCHEMA', 'T.TABLE_NAME', []), " INNER JOIN QSYS2.TABLES I ON (T.TABLE_SCHEMA = I.TABLE_SCHEMA AND T.TABLE_NAME = I.TABLE_NAME) ");
  });

  it('getTables - all', function (done) {
    db.meta.getTables(null, {}, function(err, messages, tables) {
      console.log(arguments);
      assert(!err, "Success");
      assert.notEqual(tables.length, 0, "got results");
      done();
    });
  });

  it('getTables - one', function (done) {
    db.meta.getTables({schema: 'jsharmony1', name: 'alltypes'}, {}, function(err, messages, tables) {
      console.log(arguments);
      assert(!err, "Success");
      assert.notEqual(tables.length, 0, "got results");
      done();
    });
  });

  it('getTableFields', function (done) {
    db.meta.getTableFields({schema: 'jsharmony1', name: 'alltypes'}, function(err, messages, fields) {
      console.log(fields);
      assert(!err, "Success");
      assert.notEqual(fields.length, 0, "got results");
      done();
    });
  });

  it('getForeignKeys', function (done) {
    db.meta.getForeignKeys({schema: 'jsharmony1', name: 'b'}, function(err, messages, fields) {
      console.log(fields);
      assert(!err, "Success");
      assert.notEqual(fields.length, 0, "got results");
      done();
    });
  });
});
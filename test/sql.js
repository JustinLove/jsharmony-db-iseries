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

var JSHiseriessql = require('../index');
var JSHdb = require('jsharmony-db');
var shouldGenerateFormSql = require('jsharmony-db/test/shared/sql');
var _ = require('lodash');

var dbconfig = require('./dbconfig');

dbconfig = _.extend({_driver: new JSHiseriessql(), connectionString: "DSN=ODBC;Uid=DBUSER;pwd=DBPASS", initialSize: 1, options: {pooled: true, metadata_filter: ['JSHARMONY1.%']} }, dbconfig);

var db = new JSHdb(dbconfig);
dbconfig._driver.platform.Config.debug_params.db_error_sql_state = true;
//dbconfig._driver.platform.Config.debug_params.db_raw_sql = true;

describe('iseries Forms',function(){
  this.timeout(15000);

  shouldGenerateFormSql(db, JSHdb, 'INTEGER AS IDENTITY', 'TIMESTAMP');
});
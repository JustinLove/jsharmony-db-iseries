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

var DB = require('jsharmony-db');
var _ = require('lodash');

function DBsql(db){
  this.db = db;
}

DBsql.prototype.getModelForm = function (jsh, model, selecttype, allfields, sql_allkeyfields, datalockqueries, sortfields) {
  var _this = this;
  var sql = '';

  sql = 'SELECT ';
  for (var i = 0; i < allfields.length; i++) {
    var field = allfields[i];
    if (i > 0) sql += ',';
    var fieldsql = field.name;
    if ('sqlselect' in field) fieldsql = _this.ParseSQL(field.sqlselect);
    sql += XfromDB(jsh, field, fieldsql);
    //if (field.lov) sql += ',' + _this.getLOVFieldTxt(jsh, model, field) + ' as __' + jsh.map.code_txt + '__' + field.name;
  }
  var tbl = _this.getTable(jsh, model);
  sql += ' FROM ' + tbl + ' WHERE ';
  var sqlwhere = '1=1';
  if (('sqlwhere' in model) && model.sqlwhere) sqlwhere = _this.ParseSQL(model.sqlwhere);
  sql += ' %%%SQLWHERE%%% %%%DATALOCKS%%%';

  //Add Keys to where
  _.each(sql_allkeyfields, function (field) { sql += ' AND ' + field.name + '=' + XtoDB(jsh, field, '@' + field.name); });

  if (selecttype == 'multiple') sql += ' ORDER BY %%%SORT%%%';
  if('sqlselect' in model) sql = _this.ParseSQL(model.sqlselect).replace('%%%SQL%%%', sql);

  var datalockstr = '';
  _.each(datalockqueries, function (datalockquery) { datalockstr += ' AND ' + datalockquery; });
  sql = applyDataLockSQL(sql, datalockstr);
  sql = sql.replace('%%%SQLWHERE%%%', sqlwhere);
  if (selecttype == 'multiple') {
    //Generate sort sql
    var sortstr = '';
    _.each(sortfields, function (sortfield) {
      if (sortstr != '') sortstr += ',';
      //Get sort expression
      sortstr += (sortfield.sql ? _this.ParseSQL(DB.util.ReplaceAll(sortfield.sql, '%%%SQL%%%', sortfield.field)) : sortfield.field) + ' ' + sortfield.dir;
    });
    if (sortstr == '') sortstr = '1';
    sql = sql.replace('%%%SORT%%%', sortstr);
  }

  return sql;
};

DBsql.prototype.getTable = function(jsh, model){
  var _this = this;
  if(model.table=='jsharmony:models'){
    var rslt = '';
    for(var _modelid in jsh.Models){
      var _model = jsh.Models[_modelid];
      var parents = _model._inherits.join(', ');
      if(rslt) rslt += ',';
      else rslt += '(values ';
      rslt += "(";
      rslt += "'" + _this.escape(_modelid) + "',";
      rslt += "'" + _this.escape(_model.title) + "',";
      rslt += "'" + _this.escape(_model.layout) + "',";
      rslt += "'" + _this.escape(_model.table) + "',";
      rslt += "'" + _this.escape(_model.module) + "',";
      rslt += "'" + _this.escape(parents) + "')";
    }
    rslt += ') as models(model_id,model_title,model_layout,model_table,model_module,model_parents)';
    return rslt;
  }
  return model.table;
};

DBsql.escape = function(val){
  if (val === 0) return val;
  if (val === 0.0) return val;
  if (val === "0") return val;
  if (!val) return '';
  
  if (!isNaN(val)) return val;
  
  val = val.toString();
  if (!val) return '';
  val = val.replace(/;/g, '\\;'); // this is for our line splitting, not a limit of iseries SQL.
  val = val.replace(/[\0\x01\x02\x03\x04\x05\x06\x07\x08\x0b\x0c\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f]/g, ''); // eslint-disable-line no-control-regex
  val = val.replace(/'/g, '\'\'');
  //The string delimiter for the host language and for static SQL statements is the apostrophe ('); the SQL escape character is the quotation mark (").
  return val;
};

DBsql.prototype.escape = function(val){ return DBsql.escape(val); };

DBsql.prototype.ParseBatchSQL = function(val){
  return [val];
};

DBsql.prototype.ParseSQL = function(sql){
  return this.db.ParseSQL(sql);
};

function applyDataLockSQL(sql, datalockstr){
  if (datalockstr) {
    if (!(sql.indexOf('%%%DATALOCKS%%%') >= 0)) throw new Error('SQL missing %%%DATALOCKS%%% in query: '+sql);
  }
  return DB.util.ReplaceAll(sql, '%%%DATALOCKS%%%', datalockstr||'');
}

function XfromDB(jsh, field, fieldsql){
  var rslt = fieldsql;
  if(field.type && field.sql_from_db){
    rslt = jsh.parseFieldExpression(field, field.sql_from_db, { SQL: (fieldsql?'('+fieldsql+')':'') });
  }
  //Simplify
  rslt = '(' + rslt + ') as "' + field.name + '"';

  return rslt;
}

function XtoDB(jsh, field, fieldsql){
  var rslt = fieldsql;
  if(field.type && field.sql_to_db){
    rslt = jsh.parseFieldExpression(field, field.sql_to_db, { SQL: (fieldsql?'('+fieldsql+')':'') });
  }
  return rslt;
}

exports = module.exports = DBsql;
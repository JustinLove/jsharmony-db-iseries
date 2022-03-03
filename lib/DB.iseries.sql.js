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

DBsql.prototype.getModelRecordset = function (jsh, model, sql_searchfields, allfields, sortfields, searchfields, datalockqueries, rowstart, rowcount) {
  var _this = this;
  var sql = '';
  var rowcount_sql = '';
  var sql_select_suffix = '';
  var sql_rowcount_suffix = '';

  sql_select_suffix = ' WHERE ';

  //Generate SQL Suffix (where condition)
  var sqlwhere = '1=1';
  if (('sqlwhere' in model) && model.sqlwhere) sqlwhere = _this.ParseSQL(model.sqlwhere);
  _.each(sql_searchfields, function (field) {
    if ('sqlwhere' in field) sqlwhere += ' AND ' + _this.ParseSQL(field.sqlwhere);
    else sqlwhere += ' AND ' + field.name + '=' + XtoDB(jsh, field, '@' + field.name);
  });
  sql_select_suffix += ' %%%SQLWHERE%%% %%%DATALOCKS%%% %%%SEARCH%%%';

  //Generate beginning of select statement
  sql = 'SELECT ';
  for (var i = 0; i < allfields.length; i++) {
    var field = allfields[i];
    if (i > 0) sql += ',';
    var fieldsql = field.name;
    if ('sqlselect' in field) fieldsql = _this.ParseSQL(field.sqlselect);
    sql += XfromDB(jsh, field, fieldsql);
    if (field.lov) sql += ',' + _this.getLOVFieldTxt(jsh, model, field) + ' AS "__' + jsh.map.code_txt + '__' + field.name + '"';
  }
  sql += ' FROM ' + _this.getTable(jsh, model) + ' %%%SQLSUFFIX%%% ';
  sql_rowcount_suffix = sql_select_suffix;
  sql_select_suffix += ' ORDER BY %%%SORT%%% LIMIT %%%ROWCOUNT%%% OFFSET %%%ROWSTART%%%';
  if('sqlselect' in model) sql = _this.ParseSQL(model.sqlselect).replace('%%%SQL%%%', sql);
  rowcount_sql = 'SELECT COUNT(*) AS "cnt" from ' + _this.getTable(jsh, model) + ' %%%SQLSUFFIX%%% ';
  if('sqlrowcount' in model) rowcount_sql = _this.ParseSQL(model.sqlrowcount).replace('%%%SQL%%%', rowcount_sql);

  //Generate sort sql
  var sortstr = '';
  _.each(sortfields, function (sortfield) {
    if (sortstr != '') sortstr += ',';
    //Get sort expression
    sortstr += (sortfield.sql ? _this.ParseSQL(DB.util.ReplaceAll(sortfield.sql, '%%%SQL%%%', sortfield.field)) : sortfield.field) + ' ' + sortfield.dir;
  });
  if (sortstr == '') sortstr = '1';

  var searchstr = '';
  var parseSearch = function (_searchfields) {
    var rslt = '';
    _.each(_searchfields, function (searchfield) {
      if (_.isArray(searchfield)) {
        if (searchfield.length) rslt += ' (' + parseSearch(searchfield) + ')';
      }
      else if (searchfield){
        rslt += ' ' + searchfield;
      }
    });
    return rslt;
  };
  if (searchfields.length){
    searchstr = parseSearch(searchfields);
    if(searchstr) searchstr = ' AND (' + searchstr + ')';
  }

  //Replace parameters
  sql = sql.replace('%%%SQLSUFFIX%%%', sql_select_suffix);
  sql = sql.replace('%%%ROWSTART%%%', rowstart);
  sql = sql.replace('%%%ROWCOUNT%%%', rowcount);
  sql = sql.replace('%%%SEARCH%%%', searchstr);
  sql = sql.replace('%%%SORT%%%', sortstr);
  sql = sql.replace('%%%SQLWHERE%%%', sqlwhere);
  rowcount_sql = rowcount_sql.replace('%%%SQLSUFFIX%%%', sql_rowcount_suffix);
  rowcount_sql = rowcount_sql.replace('%%%SEARCH%%%', searchstr);
  rowcount_sql = rowcount_sql.replace('%%%SQLWHERE%%%', sqlwhere);

  var datalockstr = '';
  _.each(datalockqueries, function (datalockquery) { datalockstr += ' AND ' + datalockquery; });
  sql = applyDataLockSQL(sql, datalockstr);
  rowcount_sql = applyDataLockSQL(rowcount_sql, datalockstr);

  return { sql: sql, rowcount_sql: rowcount_sql };
};

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
    if (field.lov) sql += ',' + _this.getLOVFieldTxt(jsh, model, field) + ' AS "__' + jsh.map.code_txt + '__' + field.name + '"';
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

DBsql.prototype.putModelForm = function (jsh, model, fields, keys, sql_extfields, sql_extvalues, encryptedfields, hashfields, enc_datalockqueries, param_datalocks) {
  var _this = this;
  var sql = '';
  var enc_sql = '';
  
  var fields_insert =  _.filter(fields,function(field){ return (field.sqlinsert!==''); });
  var sql_fields = _.map(fields_insert, function (field) { return field.name; }).concat(sql_extfields).join(',');
  var sql_values = _.map(fields_insert, function (field) { if(field.sqlinsert) return field.sqlinsert; return XtoDB(jsh, field, '@' + field.name); }).concat(sql_extvalues).join(',');
  var tbl = _this.getTable(jsh, model);
  sql = 'INSERT INTO ' + tbl + '(' + sql_fields + ') ';
  sql += ' VALUES(' + sql_values + ')';
  if (keys.length >= 1){
    var sqlgetinsertkeys;
    if('sqlgetinsertkeys' in model) {
      sqlgetinsertkeys = model.sqlgetinsertkeys
    } else {
      sqlgetinsertkeys = 'SELECT ' + _.map(keys, function (field) { return field.name + ' AS "' + field.name + '"'; }).join(',');
    }
    sql = sqlgetinsertkeys + ' FROM FINAL TABLE (' + sql + ')';
  }
  else sql = 'SELECT COUNT(*) "xrowcount" FROM FINAL TABLE (' + sql + ')';

  if('sqlinsert' in model){
    sql = _this.ParseSQL(model.sqlinsert).replace('%%%SQL%%%', sql);
    sql = DB.util.ReplaceAll(sql, '%%%TABLE%%%', _this.getTable(jsh, model));
    sql = DB.util.ReplaceAll(sql, '%%%FIELDS%%%', sql_fields);
    sql = DB.util.ReplaceAll(sql, '%%%VALUES%%%', sql_values);
  }

  if ((encryptedfields.length > 0) || !_.isEmpty(hashfields)) {
    enc_sql = 'UPDATE ' + tbl + ' SET ' + _.map(encryptedfields, function (field) { var rslt = field.name + '=' + XtoDB(jsh, field, '@' + field.name); return rslt; }).join(',');
    if(!_.isEmpty(hashfields)){
      if(encryptedfields.length > 0) enc_sql += ',';
      enc_sql += _.map(hashfields, function (field) { var rslt = field.name + '=' + XtoDB(jsh, field, '@' + field.name); return rslt; }).join(',');
    }
    enc_sql += ' WHERE 1=1 %%%DATALOCKS%%%';
    //Add Keys to where
    _.each(keys, function (field) {
      var cond = ' AND ' + field.name + '=' + XtoDB(jsh, field, '@' + field.name);
      enc_sql += cond;
    });
    if('sqlinsertencrypt' in model) enc_sql = _this.ParseSQL(model.sqlinsertencrypt).replace('%%%SQL%%%', enc_sql);
    
    var enc_datalockstr = '';
    _.each(enc_datalockqueries, function (datalockquery) { enc_datalockstr += ' and ' + datalockquery; });
    enc_sql = applyDataLockSQL(enc_sql, enc_datalockstr);
  }

  _.each(param_datalocks, function (param_datalock) {
    sql = addDataLockSQL(sql, "SELECT " + XtoDB(jsh, param_datalock.field, '@' + param_datalock.pname) + " as " + param_datalock.pname + " FROM SYSIBM.SYSDUMMY1", param_datalock.datalockquery);
  });
  
  return { sql: sql, enc_sql: enc_sql };
};

DBsql.prototype.postModelForm = function (jsh, model, fields, keys, sql_extfields, sql_extvalues, hashfields, param_datalocks, datalockqueries) {
  var _this = this;
  var sql = '';
  
  var tbl = _this.getTable(jsh, model);
  sql = 'UPDATE ' + tbl + ' SET ' + _.map(_.filter(fields,function(field){ return (field.sqlupdate!==''); }), function (field) { if (field && field.sqlupdate) return field.name + '=' + _this.ParseSQL(field.sqlupdate); return field.name + '=' + XtoDB(jsh, field, '@' + field.name); }).join(',');
  var sql_has_fields = (fields.length > 0);
  if (sql_extfields.length > 0) {
    var sql_extsql = '';
    for (var i = 0; i < sql_extfields.length; i++) {
      if (sql_extsql != '') sql_extsql += ',';
      sql_extsql += sql_extfields[i] + '=' + sql_extvalues[i];
    }
    if (sql_has_fields) sql += ',';
    sql += sql_extsql;
    sql_has_fields = true;
  }
  _.each(hashfields, function(field){
    if (sql_has_fields) sql += ',';
    sql += field.name + '=' + XtoDB(jsh, field, '@' + field.name);
    sql_has_fields = true;
  });
  sql += ' WHERE (%%%SQLWHERE%%%) %%%DATALOCKS%%%';
  //Add Keys to where
  _.each(keys, function (field) {
    var cond = ' and ' + field.name + '=' + XtoDB(jsh, field, '@' + field.name);
    sql += cond;
  });
  if('sqlupdate' in model) sql = _this.ParseSQL(model.sqlupdate).replace('%%%SQL%%%', sql);
  
  _.each(param_datalocks, function (param_datalock) {
    sql = addDataLockSQL(sql, "SELECT " + XtoDB(jsh, param_datalock.field, '@' + param_datalock.pname) + " AS \"" + param_datalock.pname + "\"", param_datalock.datalockquery);
  });

  var sqlwhere = '1=1';
  if(jsh && jsh.Config && jsh.Config.system_settings && jsh.Config.system_settings.deprecated && jsh.Config.system_settings.deprecated.disable_sqlwhere_on_form_update_delete){ /* Do nothing */ }
  else if (('sqlwhere' in model) && model.sqlwhere) sqlwhere = _this.ParseSQL(model.sqlwhere);
  sql = DB.util.ReplaceAll(sql, '%%%SQLWHERE%%%', sqlwhere);
  
  var datalockstr = '';
  _.each(datalockqueries, function (datalockquery) { datalockstr += ' AND ' + datalockquery; });
  sql = applyDataLockSQL(sql, datalockstr);
  
  return sql;
};

DBsql.prototype.postModelExec = function (jsh, model, param_datalocks, datalockqueries) {
  var _this = this;
  var sql = _this.ParseSQL(model.sqlexec);
  
  _.each(param_datalocks, function (param_datalock) {
    sql = addDataLockSQL(sql, "SELECT " + XtoDB(jsh, param_datalock.field, '@' + param_datalock.pname) + " AS " + param_datalock.pname, param_datalock.datalockquery);
  });
  
  var datalockstr = '';
  _.each(datalockqueries, function (datalockquery) { datalockstr += ' AND ' + datalockquery; });
  sql = applyDataLockSQL(sql, datalockstr);
  
  return sql;
};

DBsql.prototype.deleteModelForm = function (jsh, model, keys, datalockqueries) {
  var _this = this;
  var sql = '';
  
  var tbl = _this.getTable(jsh, model);
  sql += 'DELETE FROM ' + tbl + ' WHERE (%%%SQLWHERE%%%) %%%DATALOCKS%%%';
  _.each(keys, function (field) { sql += ' AND ' + field.name + '=' + XtoDB(jsh, field, '@' + field.name); });
  sql += ';';
  if('sqldelete' in model) sql = _this.ParseSQL(model.sqldelete).replace('%%%SQL%%%', sql);

  var sqlwhere = '1=1';
  if(jsh && jsh.Config && jsh.Config.system_settings && jsh.Config.system_settings.deprecated && jsh.Config.system_settings.deprecated.disable_sqlwhere_on_form_update_delete){ /* Do nothing */ }
  else if (('sqlwhere' in model) && model.sqlwhere) sqlwhere = _this.ParseSQL(model.sqlwhere);
  sql = DB.util.ReplaceAll(sql, '%%%SQLWHERE%%%', sqlwhere);
  
  var datalockstr = '';
  _.each(datalockqueries, function (datalockquery) { datalockstr += ' AND ' + datalockquery; });
  sql = applyDataLockSQL(sql, datalockstr);
  
  return sql;
};

DBsql.prototype.getDefaultTasks = function (jsh, dflt_sql_fields) {
  var _this = this;
  var sql = '';
  var sql_builder = '';

  for (var i = 0; i < dflt_sql_fields.length; i++) {
    var field = dflt_sql_fields[i];
    var fsql = XfromDB(jsh, field.field, _this.ParseSQL(field.sql));
    var datalockstr = '';
    _.each(field.datalockqueries, function (datalockquery) { datalockstr += ' AND ' + datalockquery; });
    fsql = applyDataLockSQL(fsql, datalockstr);

    _.each(field.param_datalocks, function (param_datalock) {
      sql = addDataLockSQL(sql, "SELECT " + XtoDB(jsh, param_datalock.field, '@' + param_datalock.pname) + " AS " + param_datalock.pname, param_datalock.datalockquery);
    });

    if (sql_builder) sql_builder += ',';
    sql_builder += fsql;
  }

  if (sql_builder) sql += 'SELECT ' + sql_builder + ' FROM SYSIBM.SYSDUMMY1';

  return sql;
};

DBsql.prototype.getLOVFieldTxt = function (jsh, model, field) {
  var _this = this;
  var rslt = '';
  if (!field || !field.lov) return rslt;
  var lov = field.lov;

  var valsql = field.name;
  if ('sqlselect' in field) valsql = _this.ParseSQL(field.sqlselect);

  var parentsql = '';
  if ('parent' in lov) {
    _.each(model.fields, function (pfield) {
      if (pfield.name == lov.parent) {
        if ('sqlselect' in pfield) parentsql += _this.ParseSQL(pfield.sqlselect);
        else parentsql = pfield.name;
      }
    });
    if(!parentsql && lov.parent) parentsql = lov.parent;
  }

  if(lov.values){
    if(!lov.values.length) rslt = "SELECT NULLIF(1,1) FROM SYSIBM.SYSDUMMY1";
    else if('parent' in lov) rslt = "(SELECT \"" + jsh.map.code_txt + "\" FROM (" + _this.arrayToTable(DB.util.ParseLOVValues(jsh, lov.values)) + ") "+field.name+"_values WHERE "+field.name+"_values.\""+jsh.map.code_val+"1\"=(" + parentsql + ") AND "+field.name+"_values.\""+jsh.map.code_val+"2\"=(" + valsql + "))";
    else rslt = "(SELECT \"" + jsh.map.code_txt + "\" FROM (" + _this.arrayToTable(DB.util.ParseLOVValues(jsh, lov.values)) + ") "+field.name+"_values WHERE "+field.name+"_values.\""+jsh.map.code_val+"\"=(" + valsql + "))";
  }
  else if ('sqlselect' in lov) { rslt = _this.ParseSQL(lov['sqlselect']); }
  else if ('code' in lov) { rslt = 'SELECT "' + jsh.map.code_txt + '" FROM '+(lov.schema?lov.schema+'.':'')+ jsh.map.code + '_' + lov['code'] + ' WHERE "' + jsh.map.code_val + '"=(' + valsql + ')'; }
  else if ('code2' in lov) {
    if (!parentsql) throw new Error('Parent field not found in LOV.');
    rslt = 'SELECT "' + jsh.map.code_txt + '" FROM '+(lov.schema?lov.schema+'.':'')+ jsh.map.code2 + '_' + lov['code2'] + ' WHERE "' + jsh.map.code_val + '1"=(' + parentsql + ') AND "' + jsh.map.code_val + '2"=(' + valsql + ')';
  }
  else if ('code_sys' in lov) { rslt = 'SELECT "' + jsh.map.code_txt + '" FROM '+(lov.schema?lov.schema+'.':'')+ jsh.map.code_sys + '_' + lov['code_sys'] + ' WHERE "' + jsh.map.code_val + '"=(' + valsql + ')'; }
  else if ('code2_sys' in lov) {
    if (!parentsql) throw new Error('Parent field not found in LOV.');
    rslt = 'SELECT "' + jsh.map.code_txt + '" FROM '+(lov.schema?lov.schema+'.':'')+ jsh.map.code2_sys + '_' + lov['code2_sys'] + ' WHERE "' + jsh.map.code_val + '1"=(' + parentsql + ') AND "' + jsh.map.code_val + '2"=(' + valsql + ')';
  }
  else if ('code_app' in lov) { rslt = 'SELECT "' + jsh.map.code_txt + '" FROM '+(lov.schema?lov.schema+'.':'')+ jsh.map.code_app + '_' + lov['code_app'] + ' WHERE "' + jsh.map.code_val + '"=(' + valsql + ')'; }
  else if ('code2_app' in lov) {
    if (!parentsql) throw new Error('Parent field not found in LOV.');
    rslt = 'SELECT "' + jsh.map.code_txt + '" FROM '+(lov.schema?lov.schema+'.':'')+ jsh.map.code2_app + '_' + lov['code2_app'] + ' WHERE "' + jsh.map.code_val + '1"=(' + parentsql + ') AND "' + jsh.map.code_val + '2"=(' + valsql + ')';
  }
  else rslt = "SELECT NULLIF(1,1) FROM SYSIBM.SYSDUMMY1";

  rslt = '(' + rslt + ')';
  return rslt;
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

DBsql.prototype.arrayToTable = function(table){
  var _this = this;
  var rslt = [];
  if(!table || !_.isArray(table) || !table.length) throw new Error('Array cannot be empty');
  _.each(table, function(row,i){
    var rowsql = '';
    var hasvalue = false;
    for(var key in row){
      if(rowsql) rowsql += ',';
      rowsql += "'" + _this.escape(row[key]) + "'" + ' AS "' + key + '"';
      hasvalue = true;
    }
    rowsql = 'SELECT ' + rowsql + ' FROM SYSIBM.SYSDUMMY1';
    rslt.push(rowsql);
    if(!hasvalue) throw new Error('Array row '+(i+1)+' is empty');
  });
  return rslt.join(' UNION ALL ');
};

function addDataLockSQL(sql, dsql, dquery){
  return "BEGIN IF NOT EXISTS(SELECT * FROM ("+dsql+") DUAL WHERE " + dquery + ") THEN SIGNAL SQLSTATE VALUE 'JHDLE' SET MESSAGE_TEXT = \'INVALID ACCESS'\\; END IF\\; END; " + sql;
}

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
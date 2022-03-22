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
var types = DB.types;
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

function multiCodeLOV(code, jsh, lov) {
  return 'SELECT ' + jsh.map.code_val + ',' + jsh.map.code_txt + ',' + jsh.map.code_seq + ' FROM ' + jsh.map[code] + '_' + lov[code] + ' WHERE (' + jsh.map.code_end_date + ' IS NULL OR ' + jsh.map.code_end_date + '>CURRENT_TIMESTAMP)';
}

DBsql.prototype.getModelMultisel = function (jsh, model, lovfield, allfields, sql_foreignkeyfields, datalockqueries, lov_datalockqueries, param_datalocks) {
  var _this = this;
  var sql = '';

  var tbl = _this.getTable(jsh, model);
  var tbl_alias = tbl.replace(/[^a-zA-Z0-9]+/g, '');
  if(tbl_alias.length > 50) tbl_alias = tbl_alias.substr(0,50);
  sql = 'SELECT ';
  for (var i = 0; i < allfields.length; i++) {
    var field = allfields[i];
    if (i > 0) sql += ',';
    var fieldsql = '' + field.name + '';
    if ('sqlselect' in field) fieldsql = _this.ParseSQL(field.sqlselect);
    sql += XfromDB(jsh, field, fieldsql);
  }
  sql += ' ,COALESCE(' + jsh.map.code_val + ',' + lovfield.name + ') "' + jsh.map.code_val + '",COALESCE(' + jsh.map.code_txt + ',' + jsh.map.code_val + ',' + lovfield.name + ') "' + jsh.map.code_txt + '"';
  sql += ' FROM (SELECT * FROM ' + tbl + ' WHERE 1=1 %%%DATALOCKS%%%';
  //Add Keys to where
  if (sql_foreignkeyfields.length) _.each(sql_foreignkeyfields, function (field) { sql += ' AND ' + field.name + '=' + XtoDB(jsh, field, '@' + field.name); });
  else sql += ' AND 0=1';
  sql += ') ' + tbl_alias;
  sql += ' FULL OUTER JOIN (%%%LOVSQL%%%) multiparent ON multiparent.' + jsh.map.code_val + ' = ' + tbl_alias + '.' + lovfield.name +'';
  sql += ' WHERE (%%%SQLWHERE%%%)',
  sql += ' ORDER BY ' + jsh.map.code_seq + ',' + jsh.map.code_txt;
  if('sqlselect' in model) sql = _this.ParseSQL(model.sqlselect).replace('%%%SQL%%%', sql);

  var sqlwhere = '1=1';
  if (('sqlwhere' in model) && model.sqlwhere) sqlwhere = _this.ParseSQL(model.sqlwhere);
  sql = DB.util.ReplaceAll(sql, '%%%SQLWHERE%%%', sqlwhere);

  var datalockstr = '';
  _.each(datalockqueries, function (datalockquery) { datalockstr += ' AND ' + datalockquery; });
  sql = applyDataLockSQL(sql, datalockstr);

  //Add LOVSQL to SQL
  var lovsql = '';
  var lov = lovfield.lov || {};
  if ('sql' in lov) { lovsql = lov['sql']; }
  else if ('code' in lov) { lovsql = multiCodeLOV('code', jsh, lov); }
  else if ('code_sys' in lov) { lovsql = multiCodeLOV('code_sys', jsh, lov); }
  else if ('code_app' in lov) { lovsql = multiCodeLOV('code_app', jsh, lov); }
  else throw new Error('LOV type not supported.');

  if ('sql' in lov) {
    //Add datalocks for dynamic LOV SQL
    var lov_datalockstr = '';
    _.each(lov_datalockqueries, function (datalockquery) { lov_datalockstr += ' AND ' + datalockquery; });
    lovsql = applyDataLockSQL(lovsql, lov_datalockstr);
  }

  sql = sql.replace('%%%LOVSQL%%%', lovsql);

  //Add datalocks for dynamic query string parameters
  _.each(param_datalocks, function (param_datalock) {
    sql = addDataLockSQL(sql, "SELECT " + XtoDB(jsh, param_datalock.field, '@' + param_datalock.pname) + " AS " + param_datalock.pname + " FROM SYSIBM.SYSDUMMY1", param_datalock.datalockquery );
  });

  return sql;
};

DBsql.prototype.getTabCode = function (jsh, model, selectfields, keys, datalockqueries) {
  var _this = this;
  var sql = '';

  sql = 'SELECT ';
  for (var i = 0; i < selectfields.length; i++) {
    var field = selectfields[i];
    if (i > 0) sql += ',';
    var fieldsql = field.name;
    if ('sqlselect' in field) fieldsql = _this.ParseSQL(field.sqlselect);
    sql += XfromDB(jsh, field, fieldsql);
  }
  var tbl = _this.getTable(jsh, model);
  sql += ' FROM ' + tbl + ' WHERE ';
  var sqlwhere = '1=1';
  if (('sqlwhere' in model) && model.sqlwhere) sqlwhere = _this.ParseSQL(model.sqlwhere);
  sql += ' %%%SQLWHERE%%% %%%DATALOCKS%%%';
  _.each(keys, function (field) { sql += ' AND ' + field.name + '=' + XtoDB(jsh, field, '@' + field.name); });
  if('sqlselect' in model) sql = _this.ParseSQL(model.sqlselect).replace('%%%SQL%%%', sql);

  var datalockstr = '';
  _.each(datalockqueries, function (datalockquery) { datalockstr += ' AND ' + datalockquery; });
  sql = applyDataLockSQL(sql, datalockstr);
  sql = sql.replace('%%%SQLWHERE%%%', sqlwhere);

  return sql;
};

DBsql.prototype.getTitle = function (jsh, model, sql, datalockqueries) {
  var _this = this;
  sql = _this.ParseSQL(sql);
  
  var datalockstr = '';
  _.each(datalockqueries, function (datalockquery) { datalockstr += ' AND ' + datalockquery; });
  sql = applyDataLockSQL(sql, datalockstr);
  
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
      sqlgetinsertkeys = model.sqlgetinsertkeys;
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
    sql = addDataLockSQL(sql, "SELECT " + XtoDB(jsh, param_datalock.field, '@' + param_datalock.pname) + " AS " + param_datalock.pname + " FROM SYSIBM.SYSDUMMY1", param_datalock.datalockquery);
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

function postMultiCodeLOV( code, jsh, lov ) {
  return 'SELECT ' + jsh.map.code_val + ',' + jsh.map.code_txt + ',' + jsh.map.code_seq + ' AS SEQ FROM ' + jsh.map[code] + '_' + lov[code] + ' WHERE (' + jsh.map.code_end_date + ' IS NULL OR ' + jsh.map.code_end_date + '>CURRENT_TIMESTAMP)';
}

DBsql.prototype.postModelMultisel = function (jsh, model, lovfield, lovvals, foreignkeyfields, param_datalocks, datalockqueries, lov_datalockqueries) {
  var _this = this;
  var sql = '';
  
  var tbl = _this.getTable(jsh, model);
  sql = 'BEGIN DELETE FROM ' + tbl + ' WHERE (%%%SQLWHERE%%%) ';
  _.each(foreignkeyfields, function (field) { sql += ' AND ' + field.name + '=' + XtoDB(jsh, field, '@' + field.name); });
  if (lovvals.length > 0) {
    sql += ' AND ' + lovfield.name + ' NOT IN (';
    for (let i = 0; i < lovvals.length; i++) { if (i > 0) sql += ','; sql += XtoDB(jsh, lovfield, '@multisel' + i); }
    sql += ')';
  }
  sql += ' %%%DATALOCKS%%% \\; ';
  if (lovvals.length > 0) {
    sql += 'INSERT INTO ' + tbl + '(';
    _.each(foreignkeyfields, function (field) { sql += field.name + ','; });
    sql += lovfield.name + ') SELECT ';
    _.each(foreignkeyfields, function (field) { sql += XtoDB(jsh, field, '@' + field.name) + ','; });
    sql += jsh.map.code_val + ' FROM (%%%LOVSQL%%%) MULTIPARENT WHERE ' + jsh.map.code_val + ' IN (';
    for (let i = 0; i < lovvals.length; i++) { if (i > 0) sql += ','; sql += XtoDB(jsh, lovfield, '@multisel' + i); }
    sql += ') AND ' + jsh.map.code_val + ' NOT IN (SELECT ' + lovfield.name + ' FROM ' + tbl + ' WHERE (%%%SQLWHERE%%%) ';
    _.each(foreignkeyfields, function (field) { sql += ' AND ' + field.name + '=' + XtoDB(jsh, field, '@' + field.name); });
    sql += ' %%%DATALOCKS%%%)';
  }
  else sql += 'SELECT 1 WHERE 1=0 FROM SYSIBM.SYSDUMMY1';
  sql += '\\; END';
  if('sqlupdate' in model) sql = _this.ParseSQL(model.sqlupdate).replace('%%%SQL%%%', sql);
  
  _.each(param_datalocks, function (param_datalock) {
    sql = addDataLockSQL(sql, "SELECT " + XtoDB(jsh, param_datalock.field, '@' + param_datalock.pname) + " AS " + param_datalock.pname + " FROM SYSIBM.SYSDUMMY1", param_datalock.datalockquery);
  });

  var sqlwhere = '1=1';
  if (('sqlwhere' in model) && model.sqlwhere) sqlwhere = _this.ParseSQL(model.sqlwhere);
  sql = DB.util.ReplaceAll(sql, '%%%SQLWHERE%%%', sqlwhere);
  
  var datalockstr = '';
  _.each(datalockqueries, function (datalockquery) { datalockstr += ' AND ' + datalockquery; });
  sql = applyDataLockSQL(sql, datalockstr);
  
  //Add LOVSQL to SQL
  var lovsql = '';
  var lov = lovfield.lov;
  if ('sql' in lov) { lovsql = lov['sql']; }
  else if ('code' in lov) { lovsql = postMultiCodeLOV('code', jsh, lov); }
  else if ('code_sys' in lov) { lovsql = postMultiCodeLOV('code_sys', jsh, lov); }
  else if ('code_app' in lov) { lovsql = postMultiCodeLOV('code_app', jsh, lov); }
  else throw new Error('LOV type not supported.');
  
  if ('sql' in lov) {
    var lov_datalockstr = '';
    _.each(lov_datalockqueries, function (datalockquery) { lov_datalockstr += ' AND ' + datalockquery; });
    lovsql = applyDataLockSQL(lovsql, lov_datalockstr);
  }
  sql = sql.replace('%%%LOVSQL%%%', lovsql);
  
  return sql;
};

DBsql.prototype.postModelExec = function (jsh, model, param_datalocks, datalockqueries) {
  var _this = this;
  var sql = _this.ParseSQL(model.sqlexec);
  
  _.each(param_datalocks, function (param_datalock) {
    sql = addDataLockSQL(sql, "SELECT " + XtoDB(jsh, param_datalock.field, '@' + param_datalock.pname) + " AS " + param_datalock.pname + " FROM SYSIBM.SYSDUMMY1", param_datalock.datalockquery);
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

DBsql.prototype.Download = function (jsh, model, fields, keys, datalockqueries) {
  var _this = this;
  var sql = '';
  
  var tbl = _this.getTable(jsh, model);
  sql = 'SELECT ';
  for (var i = 0; i < fields.length; i++) {
    var field = fields[i];
    if (i > 0) sql += ',';
    var fieldsql = field.name;
    if ('sqlselect' in field) fieldsql = _this.ParseSQL(field.sqlselect);
    sql += XfromDB(jsh, field, fieldsql);
  }
  sql += ' FROM ' + tbl + ' WHERE (%%%SQLWHERE%%%) %%%DATALOCKS%%%';
  //Add Keys to where
  _.each(keys, function (field) { sql += ' AND ' + field.name + '=' + XtoDB(jsh, field, '@' + field.name); });
  if('sqldownloadselect' in model) sql = _this.ParseSQL(model.sqldownloadselect).replace('%%%SQL%%%', sql);

  var sqlwhere = '1=1';
  if (('sqlwhere' in model) && model.sqlwhere) sqlwhere = _this.ParseSQL(model.sqlwhere);
  sql = DB.util.ReplaceAll(sql, '%%%SQLWHERE%%%', sqlwhere);
  
  var datalockstr = '';
  _.each(datalockqueries, function (datalockquery) { datalockstr += ' AND ' + datalockquery; });
  sql = applyDataLockSQL(sql, datalockstr);
  
  return sql;
};

DBsql.prototype.parseReportSQLData = function (jsh, dname, dparams, skipdatalock, datalockqueries) {
  var _this = this;
  var sql = _this.ParseSQL(dparams.sql);

  var datalockstr = '';
  if (!skipdatalock) _.each(datalockqueries, function (datalockquery) { datalockstr += ' AND ' + datalockquery; });
  sql = applyDataLockSQL(sql, datalockstr);

  return sql;
};

DBsql.prototype.runReportJob = function (jsh, model, datalockqueries) {
  var _this = this;
  var sql = _this.ParseSQL(model.jobqueue.sql);

  var datalockstr = '';
  _.each(datalockqueries, function (datalockquery) { datalockstr += ' AND ' + datalockquery; });
  sql = applyDataLockSQL(sql, datalockstr);

  return sql;
};

DBsql.prototype.runReportBatch = function (jsh, model, datalockqueries) {
  var _this = this;
  var sql = _this.ParseSQL(model.batch.sql);

  var datalockstr = '';
  _.each(datalockqueries, function (datalockquery) { datalockstr += ' AND ' + datalockquery; });
  sql = applyDataLockSQL(sql, datalockstr);

  return sql;
};

DBsql.prototype.getCMS_M = function (aspa_object) {
  return 'SELECT M_Desc FROM ' + aspa_object + '_M WHERE M_ID=1';
};

DBsql.prototype.getSearchTerm = function (jsh, model, field, pname, search_value, comparison) {
  var _this = this;
  var sqlsearch = '';
  var fsql = field.name;
  if (field.lov && !field.lov.showcode) fsql = _this.getLOVFieldTxt(jsh, model, field);
  if (field.sqlselect) fsql = field.sqlselect;
  if (field.sqlsearch){
    fsql = jsh.parseFieldExpression(field, _this.ParseSQL(field.sqlsearch), { SQL: fsql });
  }
  else if (field.sql_from_db){
    fsql = jsh.parseFieldExpression(field, _this.ParseSQL(field.sql_from_db), { SQL: fsql });
  }
  var ftype = field.type;
  var dbtype = null;
  var pname_param = XSearchtoDB(jsh, field, '@' + pname);
  switch (ftype) {
    case 'boolean':
      dbtype = types.Boolean;
      if (comparison == '<>') { sqlsearch = fsql + ' <> ' + pname_param; }
      else sqlsearch = fsql + ' = ' + pname_param;
      break;
    case 'bigint':
    case 'int':
    case 'smallint':
    case 'tinyint':
      dbtype = types.BigInt;
      if (comparison == '<>') { sqlsearch = fsql + ' <> ' + pname_param; }
      else if (comparison == '>') { sqlsearch = fsql + ' > ' + pname_param; }
      else if (comparison == '<') { sqlsearch = fsql + ' < ' + pname_param; }
      else if (comparison == '>=') { sqlsearch = fsql + ' >= ' + pname_param; }
      else if (comparison == '<=') { sqlsearch = fsql + ' <= ' + pname_param; }
      else sqlsearch = fsql + ' = ' + pname_param;
      break;
    case 'decimal':
    case 'float':
      if (comparison == '<>') { sqlsearch = fsql + ' <> ' + pname_param; }
      else if (comparison == '>') { sqlsearch = fsql + ' > ' + pname_param; }
      else if (comparison == '<') { sqlsearch = fsql + ' < ' + pname_param; }
      else if (comparison == '>=') { sqlsearch = fsql + ' >= ' + pname_param; }
      else if (comparison == '<=') { sqlsearch = fsql + ' <= ' + pname_param; }
      else sqlsearch = fsql + ' = ' + pname_param;
      break;
    case 'varchar':
    case 'char': //.replace(/[%_]/g,"\\$&")
      if (comparison == '=') { sqlsearch = 'UPPER(' + fsql + ') LIKE UPPER(' + pname_param+')'; }
      else if (comparison == '<>') { sqlsearch = 'UPPER(' + fsql + ') NOT LIKE UPPER(' + pname_param + ')'; }
      else if (comparison == 'notcontains') { search_value = '%' + search_value + '%'; sqlsearch = 'UPPER(' + fsql + ') NOT LIKE UPPER(' + pname_param + ')'; }
      else if (comparison == 'beginswith') { search_value = search_value + '%'; sqlsearch = 'UPPER(' + fsql + ') LIKE UPPER(' + pname_param + ')'; }
      else if (comparison == 'endswith') { search_value = '%' + search_value; sqlsearch = 'UPPER(' + fsql + ') LIKE UPPER(' + pname_param + ')'; }
      else if ((comparison == 'soundslike') && (field.sqlsearchsound)) { sqlsearch = _this.ParseSQL(field.sqlsearchsound).replace('%%%FIELD%%%', pname_param).replace('%%%SOUNDEX%%%', pname_param+'_soundex'); }
      else { search_value = '%' + search_value + '%'; sqlsearch = 'UPPER(' + fsql + ') LIKE UPPER(' + pname_param + ')'; }
      dbtype = types.VarChar(search_value.length);
      break;
    case 'datetime':
    case 'date':
      if (ftype == 'datetime') dbtype = types.DateTime(7,(field.datatype_config && field.datatype_config.preserve_timezone));
      if (comparison == '<>') { sqlsearch = fsql + ' <> ' + pname_param; }
      else if (comparison == '>') { sqlsearch = fsql + ' > ' + pname_param; }
      else if (comparison == '<') { sqlsearch = fsql + ' < ' + pname_param; }
      else if (comparison == '>=') { sqlsearch = fsql + ' >= ' + pname_param; }
      else if (comparison == '<=') { sqlsearch = fsql + ' <= ' + pname_param; }
      else sqlsearch = fsql + ' = ' + pname_param;
      break;
    case 'time':
      if (comparison == '<>') { sqlsearch = fsql + ' <> ' + pname_param; }
      else if (comparison == '>') { sqlsearch = fsql + ' > ' + pname_param; }
      else if (comparison == '<') { sqlsearch = fsql + ' < ' + pname_param; }
      else if (comparison == '>=') { sqlsearch = fsql + ' >= ' + pname_param; }
      else if (comparison == '<=') { sqlsearch = fsql + ' <= ' + pname_param; }
      else sqlsearch = fsql + ' = ' + pname_param;
      break;
    case 'hash':
      dbtype = types.VarBinary(field.length);
      if (comparison == '=') { sqlsearch = fsql + ' = ' + pname_param; }
      else if (comparison == '<>') { sqlsearch = fsql + ' <> ' + pname_param; }
      break;
    default: throw new Error('Search type ' + field.name + '/' + ftype + ' not supported.');
  }
  
  if (comparison == 'null') {
    if(_.includes(['varchar','char','binary'],ftype)) sqlsearch = "COALESCE(" + fsql + ",'')=''";
    else sqlsearch = fsql + ' IS NULL';
  }
  else if (comparison == 'notnull') {
    if(_.includes(['varchar','char','binary'],ftype)) sqlsearch = "COALESCE(" + fsql + ",'')<>''";
    else sqlsearch = fsql + ' IS NOT NULL';
  }
  
  return { sql: sqlsearch, dbtype: dbtype, search_value: search_value };
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
      sql = addDataLockSQL(sql, "SELECT " + XtoDB(jsh, param_datalock.field, '@' + param_datalock.pname) + " AS " + param_datalock.pname + " FROM SYSIBM.SYSDUMMY1", param_datalock.datalockquery);
    });

    if (sql_builder) sql_builder += ',';
    sql_builder += fsql;
  }

  if (sql_builder) sql += 'SELECT ' + sql_builder + ' FROM SYSIBM.SYSDUMMY1';

  return sql;
};

function codeLOV(code, jsh, lov) {
  return 'SELECT ' + jsh.map.code_val + ' AS "' + jsh.map.code_val + '",' + jsh.map.code_txt + ' AS "' + jsh.map.code_txt + '" FROM '+(lov.schema?lov.schema+'.':'')+ jsh.map[code] + '_' + lov[code] + ' WHERE (' + jsh.map.code_end_date + ' IS NULL OR ' + jsh.map.code_end_date + '>CURRENT_TIMESTAMP) ORDER BY ' + jsh.map.code_seq + ',' + jsh.map.code_txt;
}

function code2LOV(code2, jsh, lov) {
  return 'SELECT ' + jsh.map.code_val + '1 AS "' + jsh.map.code_parent + '",' + jsh.map.code_val + '2 as "' + jsh.map.code_val + '",' + jsh.map.code_txt + ' AS "' + jsh.map.code_txt + '" FROM '+(lov.schema?lov.schema+'.':'')+ jsh.map[code2] + '_' + lov[code2] + ' WHERE (' + jsh.map.code_end_date + ' IS NULL OR ' + jsh.map.code_end_date + '>CURRENT_TIMESTAMP) ORDER BY ' + jsh.map.code_seq + ',' + jsh.map.code_txt;
}

DBsql.prototype.getLOV = function (jsh, fname, lov, datalockqueries, param_datalocks, options) {
  var _this = this;
  options = _.extend({ truncate_lov: false }, options);
  var sql = '';
  
  if ('sql' in lov) { sql = _this.ParseSQL(lov['sql']); }
  else if ('sql2' in lov) { sql = _this.ParseSQL(lov['sql2']); }
  else if ('sqlmp' in lov) { sql = _this.ParseSQL(lov['sqlmp']); }
  else if ('code' in lov) { sql = codeLOV('code', jsh, lov); }
  else if ('code2' in lov) { sql = code2LOV('code2', jsh, lov); }
  else if ('code_sys' in lov) { sql = codeLOV('code_sys', jsh, lov); }
  else if ('code2_sys' in lov) { sql = code2LOV('code2_sys', jsh, lov); }
  else if ('code_app' in lov) { sql = codeLOV('code_app', jsh, lov); }
  else if ('code2_app' in lov) { sql = code2LOV('code2_app', jsh, lov); }
  else sql = 'SELECT 1 AS "' + jsh.map.code_val + '",1 AS "' + jsh.map.code_txt + '" FROM SYSIBM.SYSDUMMY1 WHERE 1=0';
  
  var datalockstr = '';
  _.each(datalockqueries, function (datalockquery) { datalockstr += ' AND ' + datalockquery; });
  sql = applyDataLockSQL(sql, datalockstr);

  var sqltruncate = '';
  if(options.truncate_lov){
    sqltruncate = lov.sqltruncate||'';
    if(sqltruncate.trim()) sqltruncate = ' AND '+sqltruncate;
  }
  sql = DB.util.ReplaceAll(sql, '%%%TRUNCATE%%%', sqltruncate);

  _.each(param_datalocks, function (param_datalock) {
    sql = addDataLockSQL(sql, "SELECT " + XtoDB(jsh, param_datalock.field, '@' + param_datalock.pname) + " AS " + param_datalock.pname + " FROM SYSIBM.SYSDUMMY1", param_datalock.datalockquery) ;
  });
  
  return sql;
};

function codeLOVFieldTxt(code, valsql, jsh, lov) {
  return 'SELECT ' + jsh.map.code_txt + ' AS "' + jsh.map.code_txt + '" FROM '+(lov.schema?lov.schema+'.':'')+ jsh.map[code] + '_' + lov[code] + ' WHERE ' + jsh.map.code_val + '=(' + valsql + ')';
}

function code2LOVFieldTxt(code2, parentsql, valsql, jsh, lov) {
  if (!parentsql) throw new Error('Parent field not found in LOV.');
  return 'SELECT ' + jsh.map.code_txt + ' AS "' + jsh.map.code_txt + '" FROM '+(lov.schema?lov.schema+'.':'')+ jsh.map[code2] + '_' + lov[code2] + ' WHERE ' + jsh.map.code_val + '1=(' + parentsql + ') AND ' + jsh.map.code_val + '2=(' + valsql + ')';
}

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
    else if('parent' in lov) rslt = "(SELECT " + jsh.map.code_txt + " AS \"" + jsh.map.code_txt + "\" FROM (" + _this.arrayToTable(DB.util.ParseLOVValues(jsh, lov.values)) + ") "+field.name+"_values WHERE "+field.name+"_values."+jsh.map.code_val+"1=(" + parentsql + ") AND "+field.name+"_values."+jsh.map.code_val+"2=(" + valsql + "))";
    else rslt = "(SELECT " + jsh.map.code_txt + " AS \"" + jsh.map.code_txt + "\" FROM (" + _this.arrayToTable(DB.util.ParseLOVValues(jsh, lov.values)) + ") "+field.name+"_values WHERE "+field.name+"_values."+jsh.map.code_val+"=(" + valsql + "))";
  }
  else if ('sqlselect' in lov) { rslt = _this.ParseSQL(lov['sqlselect']); }
  else if ('code' in lov) { rslt = codeLOVFieldTxt('code', valsql, jsh, lov); }
  else if ('code2' in lov) { rslt = code2LOVFieldTxt('code2', parentsql, valsql, jsh, lov); }
  else if ('code_sys' in lov) { rslt = codeLOVFieldTxt('code_sys', valsql, jsh, lov); }
  else if ('code2_sys' in lov) { rslt = code2LOVFieldTxt('code2_sys', parentsql, valsql, jsh, lov); }
  else if ('code_app' in lov) { rslt = codeLOVFieldTxt('code_app', valsql, jsh, lov); }
  else if ('code2_app' in lov) { rslt = code2LOVFieldTxt('code2_app', parentsql, valsql, jsh, lov); }
  else rslt = "SELECT NULLIF(1,1) FROM SYSIBM.SYSDUMMY1";

  rslt = '(' + rslt + ')';
  return rslt;
};

// note: breadcrumbs sql expressions will need to alias quoted fields to get lower case
DBsql.prototype.getBreadcrumbTasks = function (jsh, model, sql, datalockqueries, bcrumb_sql_fields) {
  console.log(model, sql, datalockqueries, bcrumb_sql_fields);
  var _this = this;
  sql = _this.ParseSQL(sql);
  if(sql.indexOf('%%%DATALOCKS%%%') >= 0){
    //Standard Datalock Implementation
    var datalockstr = '';
    _.each(datalockqueries, function (datalockquery) { datalockstr += ' AND ' + datalockquery; });
    sql = applyDataLockSQL(sql, datalockstr);
  }
  else {
    //Pre-check Parameters for Stored Procedure execution
    _.each(datalockqueries, function (datalockquery) {
      sql = addDataLockSQL(sql, "%%%BCRUMBSQLFIELDS%%%", datalockquery);
    });
    if (bcrumb_sql_fields.length) {
      var bcrumb_sql = 'SELECT ';
      for (var i = 0; i < bcrumb_sql_fields.length; i++) {
        var field = bcrumb_sql_fields[i];
        if (i > 0) bcrumb_sql += ',';
        bcrumb_sql += XtoDB(jsh, field, '@' + field.name) + " AS \"" + field.name + "\"";
      }
      sql = sql + ' FROM SYSIBM.SYSDUMMY1';
      sql = DB.util.ReplaceAll(sql, '%%%BCRUMBSQLFIELDS%%%', bcrumb_sql);
    }
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
      else rslt += '(VALUES ';
      rslt += "(";
      rslt += "'" + _this.escape(_modelid) + "',";
      rslt += "'" + _this.escape(_model.title) + "',";
      rslt += "'" + _this.escape(_model.layout) + "',";
      rslt += "'" + _this.escape(_model.table) + "',";
      rslt += "'" + _this.escape(_model.module) + "',";
      rslt += "'" + _this.escape(parents) + "')";
    }
    rslt += ') AS MODELS(model_id,model_title,model_layout,model_table,model_module,model_parents)';
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
      rowsql += "'" + _this.escape(row[key]) + "'" + ' AS ' + key;
      hasvalue = true;
    }
    rowsql = 'SELECT ' + rowsql + ' FROM SYSIBM.SYSDUMMY1';
    rslt.push(rowsql);
    if(!hasvalue) throw new Error('Array row '+(i+1)+' is empty');
  });
  return rslt.join(' UNION ALL ');
};

function addDataLockSQL(sql, dsql, dquery){
  return "BEGIN IF NOT EXISTS(SELECT * FROM ("+dsql+") DUAL WHERE " + dquery + ") THEN SIGNAL SQLSTATE VALUE 'JHDLE' SET MESSAGE_TEXT = 'INVALID ACCESS'\\; END IF\\; END; " + sql;
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

function XSearchtoDB(jsh, field, fieldsql){
  var rslt = fieldsql;
  if(field.type && field.sqlsearch_to_db){
    rslt = jsh.parseFieldExpression(field, field.sqlsearch_to_db, { SQL: (fieldsql?'('+fieldsql+')':'') });
  }
  return rslt;
}

exports = module.exports = DBsql;
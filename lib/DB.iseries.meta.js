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
var dbtypes = DB.types;
var _ = require('lodash');

function DBmeta(db){
  this.db = db;
}

DBmeta.prototype.hasMeta = function(metadata_filter){
  return (metadata_filter && metadata_filter.length);
}

DBmeta.prototype.metaIncludeJoin = function(schema_column, table_column, metadata_filter) {
  // QSYS2.TABLES only includes tables we can access, this could cut us from tens of thousands to a few hundred
  if (_.includes(metadata_filter, '%.%')) {
    return " INNER JOIN QSYS2.TABLES I ON (" + schema_column + " = I.TABLE_SCHEMA AND " + table_column + " = I.TABLE_NAME) ";
  }

  return "";
};

DBmeta.prototype.metaInclude = function(schema_column, table_column, metadata_filter) {
  var _this = this;

  if(!_this.hasMeta(metadata_filter)){
    return "(0=1)";
  }

  if (_.includes(metadata_filter, '%.%')) {
    return "(" + table_column + " NOT IN ('SYSIBM', 'SYSIBMADM', 'SYSTOOLS'))";
  }

  var driver = _this.db.dbconfig._driver;
  var schemas = [];
  var tables = [];
  for(var i=0;i<metadata_filter.length;i++){
    var parts = metadata_filter[i].toUpperCase().split('.');
    if (parts.length != 2) {
      _this.db.platform.Log.warning("Invalid metadata_filter value: ", metadata_filter[i]);
      continue;
    }
    if (parts[1] == '%') {
      schemas.push(driver.getDBParam(dbtypes.VarChar(dbtypes.MAX), parts[0]));
    } else {
      tables.push("(" + schema_column + " = " + driver.getDBParam(dbtypes.VarChar(dbtypes.MAX), parts[0]) + " AND " + table_column + " = " + driver.getDBParam(dbtypes.VarChar(dbtypes.MAX), parts[1]) + ")");
    }
  }

  if(schemas.length) tables.push(schema_column + " IN (" + schemas.join(",") + ")");

  return '(' + tables.join(' OR ') + ')';
};

DBmeta.prototype.getTables = function(table, options, callback){
  var _this = this;
  options = _.extend({ ignore_jsharmony_schema: true }, options);

  var tables = [];
  var messages = [];
  if(!_this.hasMeta(_this.db.dbconfig.options.metadata_filter)) return callback(null, messages, tables);
  var sql_param_types = [];
  var sql_params = {};
  var sql = [
    "SELECT T.TABLE_SCHEMA \"schema_name\", T.TABLE_NAME \"table_name\", T.LONG_COMMENT \"description\", T.TABLE_TYPE \"table_type\" ",
    "FROM QSYS2.SYSTABLES T ",
    _this.metaIncludeJoin('T.TABLE_SCHEMA', 'T.TABLE_NAME', _this.db.dbconfig.options.metadata_filter),
    "WHERE 1=1 ",
  ].join(' ');
  if(table){
    sql += "AND T.TABLE_NAME=@table_name AND T.TABLE_SCHEMA=@schema_name";
    sql_param_types = [dbtypes.VarChar(dbtypes.MAX), dbtypes.VarChar(dbtypes.MAX)];
    sql_params = {'schema_name':(table.schema||_this.db.getDefaultSchema()).toUpperCase(),'table_name':table.name.toUpperCase()};
  } else {
    sql += "AND " + _this.metaInclude('T.TABLE_SCHEMA', 'T.TABLE_NAME', _this.db.dbconfig.options.metadata_filter);
  }
  sql += " ORDER BY T.TABLE_SCHEMA,T.TABLE_NAME;";
  this.db.Recordset('',sql,sql_param_types,sql_params,function(err,rslt){
    if(err){ err.sql = sql; return callback(err); }
    for(var i=0;i<rslt.length;i++){
      var dbtable = rslt[i];
      if(!table){
        if(options.ignore_jsharmony_schema && (dbtable.schema_name == 'JSHARMONY')) continue;
      }
      var table_selector = dbtable.table_name;
      if(dbtable.schema_name && (dbtable.schema_name != _this.db.getDefaultSchema())) table_selector = dbtable.schema_name + '.' + dbtable.table_name;
      tables.push({
        schema:dbtable.schema_name,
        name:dbtable.table_name,
        description:dbtable.description,
        table_type:(dbtable.table_type == 'V' ? 'view' : 'table'),
        model_name:(dbtable.schema_name==_this.db.getDefaultSchema()?dbtable.table_name:dbtable.schema_name+'_'+dbtable.table_name),
        table_selector: table_selector,
      });
    }
    return callback(null, messages, tables);
  });
};

DBmeta.prototype.getTableFields = function(tabledef, callback){
  var _this = this;
  var fields = [];
  var messages = [];
  var tableparams = { 'schema_name':null,'table_name':null };
  if(tabledef) tableparams = {'schema_name':(tabledef.schema||_this.db.getDefaultSchema()).toUpperCase(),'table_name':tabledef.name?tabledef.name.toUpperCase():null};

  if(!_this.hasMeta(_this.db.dbconfig.options.metadata_filter)) return callback(null, messages, fields);

  var sql = [
    "SELECT",
    "  C.TABLE_SCHEMA \"schema_name\",",
    "  C.TABLE_NAME \"table_name\",",
    "  COLUMN_NAME \"column_name\",",
    "  DATA_TYPE \"type_name\",",
    "  CHARACTER_MAXIMUM_LENGTH \"max_length\",",
    "  COALESCE(NUMERIC_PRECISION,	DATETIME_PRECISION) \"precision\",",
    "  NUMERIC_SCALE \"scale\",",
    "  CASE WHEN HAS_DEFAULT = 'N' AND IS_NULLABLE = 'N' THEN 1 ELSE 0 END \"required\",",
    "  CASE IS_UPDATABLE WHEN 'N' THEN 1 ELSE 0 END \"readonly\",",
    "  C.LONG_COMMENT \"description\",",
    "  CASE IS_IDENTITY WHEN 'YES' THEN 1 ELSE 0 END \"primary_key\"",
    "FROM QSYS2.SYSCOLUMNS C ",
    _this.metaIncludeJoin('C.TABLE_SCHEMA', 'C.TABLE_NAME', _this.db.dbconfig.options.metadata_filter),
    "WHERE " + _this.metaInclude('C.TABLE_SCHEMA', 'C.TABLE_NAME', _this.db.dbconfig.options.metadata_filter),
    "  AND C.TABLE_SCHEMA = COALESCE(@schema_name, C.TABLE_SCHEMA)",
    "  AND C.TABLE_NAME = COALESCE(@table_name, C.TABLE_NAME)",
    "ORDER BY C.TABLE_SCHEMA, C.TABLE_NAME, ORDINAL_POSITION",
  ].join(' ');
  _this.db.Recordset('',sql,[dbtypes.VarChar(dbtypes.MAX), dbtypes.VarChar(dbtypes.MAX)],tableparams,function(err,rslt){
    if(err){ err.sql = sql; return callback(err); }
    //Convert to jsHarmony Data Types / Fields
    for(var i=0;i<rslt.length;i++){
      var col = rslt[i];
      var field = { name: col.column_name };
      if(col.type_name=="SMALLINT"){ field.type = "smallint"; }
      else if(col.type_name=="INTEGER"){ field.type = "integer"; }
      else if(col.type_name=="BIGINT"){ field.type = "bigint"; }
      else if(col.type_name=="DECIMAL"){ field.type = "decimal"; field.precision = [col.precision, col.scale]; }
      else if(col.type_name=="NUMERIC"){ field.type = "numeric"; field.precision = [col.precision, col.scale]; }
      else if(col.type_name=="FLOAT"){
        // percision <= 24 is single-precision/real
        // > 24 is double precision; they both reflect as float
        field.type = "float";
        // docs say 53, pub db returns 52
        if(col.precision != 52 && col.precision != 53) field.precision = col.precision;
      }
      else if(col.type_name=="DECFLOAT"){
        field.type = "decfloat";
        field.precision = col.precision;
      }
      // single byte characters
      else if(col.type_name=="CHAR"){ field.type = "char"; field.length = col.max_length; }
      else if(col.type_name=="VARCHAR"){ field.type = "varchar"; field.length = col.max_length; }
      else if(col.type_name=="CLOB"){ field.type = "clob"; field.length = col.max_length; }
      // double byte characters (NCHAR also reflects as GRAPHIC)
      else if(col.type_name=="GRAPHIC"){ field.type = "graphic"; field.length = col.max_length; }
      else if(col.type_name=="VARG"){ field.type = "varg"; field.length = col.max_length; }
      else if(col.type_name=="DBCLOB"){ field.type = "dbclob"; field.length = col.max_length; }

      else if(col.type_name=="BINARY"){ field.type = "binary"; field.length = col.max_length; }
      else if(col.type_name=="VARBIN"){ field.type = "varbin"; field.length = col.max_length; }
      else if(col.type_name=="BLOB"){ field.type = "blob"; field.length = col.max_length; }
      else if(col.type_name=="DATE"){ field.type = "date"; field.precision = col.precision; }
      else if(col.type_name=="TIME"){ field.type = "time"; field.precision = col.precision; }
      else if(col.type_name=="TIMESTMP"){ field.type = "timestamp"; field.precision = col.precision; }
      else if(col.type_name=="DATALINK"){ field.type = "datalink"; }
      else if(col.type_name=="ROWID"){
        field.type = "rowid";
        field.length = col.max_length;
      }
      else if(col.type_name=="XML"){ field.type = "xml"; }
      else{
        messages.push('WARNING - Skipping Column: '+col.schema_name+'.'+col.table_name+'.'+col.column_name+': Data type '+col.type_name + ' not supported.');
        continue;
      }
      field.coldef = col;
      fields.push(field);
    }
    return callback(null, messages, fields);
  });
};

DBmeta.prototype.getForeignKeys = function(tabledef, callback){
  var _this = this;
  var fields = [];
  var messages = [];
  var tableparams = { 'schema_name':null,'table_name':null };
  if(tabledef) tableparams = {'schema_name':(tabledef.schema||_this.db.getDefaultSchema()).toUpperCase(),'table_name':tabledef.name?tabledef.name.toUpperCase():null};
  if(!_this.hasMeta(_this.db.dbconfig.options.metadata_filter)) return callback(null, messages, fields);
  var sql = [
    "SELECT",
    "  FKTABLE_SCHEM \"child_schema\",",
    "  FKTABLE_NAME \"child_table\",",
    "  FKCOLUMN_NAME \"child_column\",",
    "  PKTABLE_SCHEM \"parent_schema\",",
    "  PKTABLE_NAME \"parent_table\",",
    "  PKCOLUMN_NAME \"parent_column\" ",
    "FROM SYSIBM.SQLFOREIGNKEYS",
    _this.metaIncludeJoin('FKTABLE_SCHEM', 'FKTABLE_NAME', _this.db.dbconfig.options.metadata_filter),
    "WHERE FKTABLE_SCHEM = COALESCE(@schema_name, FKTABLE_SCHEM)",
    "  AND FKTABLE_NAME = COALESCE(@table_name, FKTABLE_NAME)",
    "  AND " + _this.metaInclude('FKTABLE_SCHEM', 'FKTABLE_NAME', _this.db.dbconfig.options.metadata_filter),
    "ORDER BY FKTABLE_SCHEM, FKTABLE_NAME, FKCOLUMN_NAME",
  ].join(' ');
  _this.db.Recordset('',sql,[dbtypes.VarChar(dbtypes.MAX), dbtypes.VarChar(dbtypes.MAX)],tableparams,function(err,rslt){
    if(err){ err.sql = sql; return callback(err); }

    //Convert to jsHarmony Data Types / Fields
    for(var i=0;i<rslt.length;i++){
      var col = rslt[i];
      var field = {
        from: {
          schema_name: col.child_schema,
          table_name: col.child_table,
          column_name: col.child_column
        },
        to: {
          schema_name: col.parent_schema,
          table_name: col.parent_table,
          column_name: col.parent_column
        }
      };
      fields.push(field);
    }
    return callback(null, messages, fields);
  });
};

exports = module.exports = DBmeta;
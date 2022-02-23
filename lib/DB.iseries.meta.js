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

DBmeta.prototype.getTables = function(table, options, callback){
  var _this = this;
  options = _.extend({ ignore_jsharmony_schema: true }, options);

  var tables = [];
  var messages = [];
  var sql_param_types = [];
  var sql_params = {};
  // SYSTABLES has better information, but the ANS/ISO TABLES only lists things we can use
  var sql = "SELECT T.TABLE_SCHEMA \"schema_name\", T.TABLE_NAME \"table_name\", T.LONG_COMMENT \"description\", T.TABLE_TYPE \"table_type\" \
    FROM QSYS2.SYSTABLES T INNER JOIN QSYS2.TABLES I ON (T.TABLE_SCHEMA = I.TABLE_SCHEMA AND T.TABLE_NAME = I.TABLE_NAME) \
    WHERE T.TABLE_TYPE IN ('T', 'V') AND T.FILE_TYPE = 'D' AND T.TABLE_SCHEMA NOT LIKE 'Q%' AND T.TABLE_SCHEMA NOT IN ('SYSIBM', 'SYSIBMADM', 'SYSTOOLS') \
      ";
  if(table){
    sql += "AND T.TABLE_NAME=@table_name AND T.TABLE_SCHEMA=@schema_name";
    sql_param_types = [dbtypes.VarChar(dbtypes.MAX), dbtypes.VarChar(dbtypes.MAX)];
    sql_params = {'schema_name':(table.schema||_this.db.getDefaultSchema()).toUpperCase(),'table_name':table.name.toUpperCase()};
  }
  sql += " ORDER BY T.TABLE_SCHEMA,T.TABLE_NAME;";
  this.db.Recordset('',sql,sql_param_types,sql_params,function(err,rslt){
    if(err){ return callback(err); }
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
  _this.db.Recordset('',"SELECT\
  C.TABLE_SCHEMA \"schema_name\",\
  C.TABLE_NAME \"table_name\",\
  COLUMN_NAME \"column_name\",\
  DATA_TYPE \"type_name\",\
  CHARACTER_MAXIMUM_LENGTH \"max_length\",\
  COALESCE(NUMERIC_PRECISION,	DATETIME_PRECISION) \"precision\",\
  NUMERIC_SCALE \"scale\",\
  CASE WHEN HAS_DEFAULT = 'N' AND IS_NULLABLE = 'N' THEN 1 ELSE 0 END \"required\",\
  CASE IS_UPDATABLE WHEN 'N' THEN 1 ELSE 0 END \"readonly\",\
  T.LONG_COMMENT \"description\",\
  CASE IS_IDENTITY WHEN 'Y' THEN 1 ELSE 0 END \"primary_key\" \
FROM QSYS2.SYSCOLUMNS C INNER JOIN QSYS2.SYSTABLES T ON (T.TABLE_SCHEMA = C.TABLE_SCHEMA AND T.TABLE_NAME = C.TABLE_NAME) INNER JOIN QSYS2.TABLES I ON (T.TABLE_SCHEMA = I.TABLE_SCHEMA AND T.TABLE_NAME = I.TABLE_NAME) \
WHERE C.TABLE_SCHEMA = COALESCE(@schema_name, C.TABLE_SCHEMA) \
   AND C.TABLE_NAME = COALESCE(@table_name, C.TABLE_NAME) \
   AND C.TABLE_SCHEMA NOT LIKE 'Q%' AND C.TABLE_SCHEMA NOT IN ('SYSIBM', 'SYSIBMADM', 'SYSTOOLS') \
ORDER BY C.TABLE_SCHEMA, C.TABLE_NAME, ORDINAL_POSITION \
  ",
  [dbtypes.VarChar(dbtypes.MAX), dbtypes.VarChar(dbtypes.MAX)],
  tableparams,
  function(err,rslt){
    if(err){ return callback(err); }
    //Convert to jsHarmony Data Types / Fields
    for(var i=0;i<rslt.length;i++){
      var col = rslt[i];
      var field = { name: col.column_name };
      if(col.type_name=="ROWID"){
        field.type = "varchar";
        field.length = col.max_length;
      }
      else if(col.type_name=="SMALLINT"){ field.type = "smallint"; }
      else if(col.type_name=="INTEGER"){ field.type = "int"; }
      else if(col.type_name=="BIGINT"){ field.type = "bigint"; }
      else if(col.type_name=="DECIMAL"){ field.type = "decimal"; field.precision = [col.precision, col.scale]; }
      else if(col.type_name=="NUMERIC"){ field.type = "decimal"; field.precision = [col.precision, col.scale]; }
      else if(col.type_name=="FLOAT"){
        if(field.precision <= 24){
          field.type = "real";
          if(col.precision != 24) field.precision = col.precision;
        }
        else {
          field.type = "double precision";
          // docs say 53, pub db returns 52
          if(col.precision != 52 && col.precision != 53) field.precision = col.precision;
        }
      }
      else if(col.type_name=="DECFLOAT"){
        field.type = "double precision";
        field.precision = col.precision;
      }
      // single byte characters
      else if(col.type_name=="CHAR"){ field.type = "char"; field.length = col.max_length; }
      else if(col.type_name=="VARCHAR"){ field.type = "varchar"; field.length = col.max_length; }
      else if(col.type_name=="CLOB"){ field.type = "varchar"; field.length = col.maxlength; }
      // double byte characters (NCHAR also reflects as GRAPHIC)
      else if(col.type_name=="GRAPHIC"){ field.type = "char"; field.length = col.max_length; }
      else if(col.type_name=="VARG"){ field.type = "varchar"; field.length = col.max_length; }
      else if(col.type_name=="DBCLOB"){ field.type = "varchar"; field.length = col.max_length; }

      else if(col.type_name=="BINARY"){ field.type = "binary"; field.length = col.max_length; }
      else if(col.type_name=="VARBIN"){ field.type = "varbinary"; field.length = col.max_length; }
      else if(col.type_name=="BLOB"){ field.type = "varbinary"; field.length = col.max_length; }
      else if(col.type_name=="DATE"){ field.type = "date"; field.precision = col.precision; }
      else if(col.type_name=="TIME"){ field.type = "time"; field.precision = col.precision; }
      else if(col.type_name=="TIMESTMP"){ field.type = "timestamp"; field.precision = col.precision; }
      else if(col.type_name=="DATALINK"){ field.type = "datalink"; }
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
  _this.db.Recordset('',"SELECT\
  FKTABLE_SCHEM \"child_schema\",\
  FKTABLE_NAME \"child_table\",\
  FKCOLUMN_NAME \"child_column\",\
  PKTABLE_SCHEM \"parent_schema\",\
  PKTABLE_NAME \"parent_table\",\
  PKCOLUMN_NAME \"parent_column\" \
FROM SYSIBM.SQLFOREIGNKEYS INNER JOIN QSYS2.SYSTABLES T ON \(T.TABLE_SCHEMA = FKTABLE_SCHEM AND T.TABLE_NAME = FKTABLE_NAME) INNER JOIN QSYS2.TABLES I ON (T.TABLE_SCHEMA = I.TABLE_SCHEMA AND T.TABLE_NAME = I.TABLE_NAME)\
WHERE FKTABLE_SCHEM = COALESCE(@schema_name, FKTABLE_SCHEM) \
  AND FKTABLE_NAME = COALESCE(@table_name, FKTABLE_NAME) \
  AND FKTABLE_SCHEM NOT LIKE 'Q%' AND FKTABLE_SCHEM NOT IN ('SYSIBM', 'SYSIBMADM', 'SYSTOOLS') \
ORDER BY FKTABLE_SCHEM, FKTABLE_NAME, FKCOLUMN_NAME\
                        ",
  [dbtypes.VarChar(dbtypes.MAX), dbtypes.VarChar(dbtypes.MAX)],
  tableparams,
  function(err,rslt){
    if(err){ return callback(err); }

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
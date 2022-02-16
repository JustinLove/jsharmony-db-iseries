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

function DBsql(db){
  this.db = db;
}

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

exports = module.exports = DBsql;
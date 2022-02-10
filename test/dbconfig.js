var path = require('path');
var fs = require('fs');
var os = require('os');

var dbconfig = { };

var path_TestDBConfig = path.join(os.homedir(),'jsharmony/testdb_iseries.json');
if(fs.existsSync(path_TestDBConfig)){
  dbconfig = JSON.parse(fs.readFileSync(path_TestDBConfig,'utf8'));
  console.log('\r\n==== Loading test database config ====\r\n'+JSON.stringify(dbconfig,null,4)+'\r\n');
}

module.exports = dbconfig;
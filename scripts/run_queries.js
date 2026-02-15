const sql=require("mssql"),fs=require("fs");
var ev=fs.readFileSync("/Users/eddiemenezes/Documents/New project/sozo/.env.local","utf8");
var env={};ev.split(String.fromCharCode(10)).forEach(function(l){var m=l.match(/^([^#=]+)=(.*)$/);if(m)env[m[1].trim()]=m[2].trim()});
var cfg={server:env.SOZO_SQL_HOST,database:"sozov2",user:env.SOZO_SQL_USER,password:env.SOZO_SQL_PASSWORD,options:{encrypt:true,trustServerCertificate:false,requestTimeout:120000},pool:{max:3}};
var Q=JSON.parse(fs.readFileSync("/Users/eddiemenezes/Documents/New project/sozo/scripts/sozov2q_queries.json","utf8"));
function pt(R,T){console.log();console.log(T);console.table(R)}
async function main(){var pool=await sql.connect(cfg);for(var q of Q){try{var r=await pool.request().query(q.sql);pt(r.recordset,q.title)}catch(e){console.log(q.title,e.message)}}await pool.close()}
main().catch(function(e){console.error(e);process.exit(1)})
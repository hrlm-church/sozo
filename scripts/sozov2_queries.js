const sql = require("mssql");
const fs = require("fs");

const envFile = fs.readFileSync("/Users/eddiemenezes/Documents/New project/sozo/.env.local", "utf8");
const env = {};
envFile.split(String.fromCharCode(10)).forEach(l => {
  const m = l.match(/^([^#=]+)=(.*)$/);
  if (m) env[m[1].trim()] = m[2].trim();
});

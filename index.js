const express = require('express');
const cors = require('cors');
let app = express();

const aws = require('aws-sdk');
aws.config.update({region: 'us-east-1'});
const AthenaExpress = require('athena-express');

const athenaExpressConfig = {
	aws,
	s3: 's3://aws-athena-query-results-528165410097-us-east-1',
	db: 'weather',
	getStats: true
};

const athenaExpress = new AthenaExpress(athenaExpressConfig);

app.use(cors());

app.get('/currentTemp', function (req, res, next) {
  const queryObject = {sql:
    `select temperature
     from dailyweather
     order by date_parse(rundate, '%Y-%m-%d %H:%i:%S') desc
     limit 1;`};
  getCurrentTemp(queryObject, res);
});

async function getCurrentTemp(queryObject, res) {
  const query = await athenaExpress.query(queryObject);
	console.log(query.QueryCostInUSD);
  res.send(query.Items[0].temperature);
}

app.get('/trailingTemps', function (req, res, next) {
  const queryObject = {sql:
    `select date_format(
                cast(date_parse(rundate,
                                '%Y-%m-%d %H:%i:%S')
                     as timestamp with time zone) at time zone 'America/Denver',
                '%l %p'),
					  temperature,
						description
     from dailyweather
     order by date_parse(rundate, '%Y-%m-%d %H:%i:%S') desc
     limit 13;`};
  getTrailingTemps(queryObject, res);
});

async function getTrailingTemps(queryObject, res) {
  const query = await athenaExpress.query(queryObject);
	console.log(query.QueryCostInUSD);
  res.send(query.Items);
}

app.get('/summary', function (req, res, next) {
  const queryObject = {sql:
    `select *
     from dailyweather
     order by date_parse(rundate, '%Y-%m-%d %H:%i:%S') desc
     limit 50;`};
  getSummary(queryObject, res);
});

async function getSummary(queryObject, res) {
  const query = await athenaExpress.query(queryObject);
	console.log(query.QueryCostInUSD);
  res.send(query.Items);
}

app.get('/highsLows', function (req, res, next) {
  const queryObject = {sql:
    `select cast(date_parse(rundate, '%Y-%m-%d %H:%i:%S') as date) as day,
						date_format(cast(date_parse(rundate, '%Y-%m-%d %H:%i:%S') as date), '%W') as day,
						max(temperature) as high,
						min(temperature) as low
		 from dailyweather
		 group by cast(date_parse(rundate, '%Y-%m-%d %H:%i:%S') as date),
						  date_format(cast(date_parse(rundate, '%Y-%m-%d %H:%i:%S') as date), '%W')
		 order by cast(date_parse(rundate, '%Y-%m-%d %H:%i:%S') as date) desc
		 limit 7;`};
  getHighsLows(queryObject, res);
});

async function getHighsLows(queryObject, res) {
  const query = await athenaExpress.query(queryObject);
	console.log(query.QueryCostInUSD);
  res.send(query.Items);
}

app.listen(4100, function () {
  console.log('CORS-enabled web server listening on port 4100');
});

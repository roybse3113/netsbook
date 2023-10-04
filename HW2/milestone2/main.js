/**
 * 
 */

var port = 8081;

const express = require('express');
const app = new express();
const path = require("path");
const JSON5 = require("json5")
const stemmer = require("stemmer")

const AWS = require("aws-sdk");

const {DynamoDB, QueryCommand} = require('@aws-sdk/client-dynamodb-v2-node');

AWS.config.update({region:'us-east-1'});

const client = new AWS.DynamoDB();

app.set("view engine", "pug");
app.set("views", path.join(__dirname, "views"))

app.get('/', function(request, response) {
    response.sendFile('html/index.html', { root: __dirname });
});

app.get('/talk', function(request, response) {
	// call the document client
  var docClient = new AWS.DynamoDB.DocumentClient();
  var talkID = parseInt(request.query.id);
  //outlien parameters
  const params = {
	KeyConditionExpression: '#talk_id = :talk_id',
	ExpressionAttributeNames: {
		'#talk_id' : 'talk_id'
	},
	ExpressionAttributeValues: {
		':talk_id': talkID,
	},
	TableName: 'ted_talks',
  };
  
  //to return result list
  var results = []
  
  //query the table using parameters
  docClient.query(params).promise().then(
		data => {
			var result = data.Items[0];
			// parse for topics
			result.topics = JSON5.parse(result.topics);
			result.related_talks = result.related_talks.replace(/(\d+)(:)/g, "'$1':");
			// get rid of 's that causes bug'
			result.related_talks = result.related_talks.replace(/'s/g, 's');
			// get rid of letter then colon that causes bug
			result.related_talks = result.related_talks.replace(/[A-Za-z]:/g, '');
			// parse for related talks
			result.related_talks = JSON5.parse(result.related_talks);
			results.push(result);
			response.render("results", { "search": talkID, "results": results});
		},
		err => {
			console.log(err);
		}
	)
	
  
})

app.get('/talks', async function(request, response) {
  var docClient = new AWS.DynamoDB.DocumentClient();
  // separate all of the words to contain multiple words in one search
  var words = request.query.keyword.split(' ');
  // one for index of talks one for talks themselves
  var promises = [];
  var resultPromises = [];
	

  words.forEach((word) => {
	//prepare word to match inverse table entries
	  word = word.trim();
	  word = word.toLowerCase();
	  word = stemmer(word);
	  const params = {
		KeyConditionExpression: 'keyword = :v_keyword',
		ExpressionAttributeValues: {
			':v_keyword' : word,
		},
		TableName: 'inverted',
	  };
	  //add all promises to list
	  promises.push(docClient.query(params).promise());
  });
  
  // await promise in order to process sequentially
  const first = await Promise.all(promises).then(
	data => {
		var indices = [];
		data.forEach(dat => {
			var i = 0;
			// less than 15 talks
			while (i < dat.Items.length && indices.length <= 15) {
				//no duplicates
				if (!indices.includes(dat.Items[i].inxid)) {
					indices.push(dat.Items[i].inxid);
				}
				i++;
			}
		})
		return indices;
	},
	err => {
		console.log(err);
  	});
  	
    
  first.forEach(talkID => {
	//parameters for indices
	const params = {
		KeyConditionExpression: '#talk_id = :talk_id',
		ExpressionAttributeNames: {
			'#talk_id' : 'talk_id'
		},
		ExpressionAttributeValues: {
			':talk_id': talkID,
		},
		TableName: 'ted_talks',
	  };
	  //add all promises
	  resultPromises.push(docClient.query(params).promise());
  });
  
	//await promises in order to sequentially compute
	var results = await Promise.all(resultPromises).then(
	data => {
		var res = [];
		data.forEach(dat => {
			// do same thing for /talk but for each data point
			var result = dat.Items[0];
			result.topics = JSON5.parse(result.topics);
			result.related_talks = result.related_talks.replace(/(\d+)(:)/g, "'$1':");
			result.related_talks = result.related_talks.replace(/'s/g, 's');
			result.related_talks = result.related_talks.replace(/[A-Za-z]:/g, '');
			result.related_talks = JSON5.parse(result.related_talks);
			res.push(result);
		})
		return res;
	},
	err => {
		console.log(err);
  	});
  	
  	//render results

	response.render("results", { "search": request.query.keyword, "results": results});

});

app.listen(port, () => {
  console.log(`HW2 app listening at http://localhost:${port}`)
})

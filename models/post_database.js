var AWS = require('aws-sdk'); 
AWS.config.update({region:'us-east-1'});
var db = new AWS.DynamoDB();
 

var get_post = function(postid, callback) {  
    // parameters to check if user already exists with username
    var params = {
      KeyConditions: {
        "ID": {
          ComparisonOperator: 'EQ',
          AttributeValueList: [ { N: postid } ]
        },
      },
      TableName: "posts",
    };
  
    // return all matching users
    db.query(params, function(err, data) {
      if (err) {
        callback(err, "Error: database.js: getPostByID: " + err);
      } else if (data.Count == 0 || data.Items.length === 0) {
          callback(err, null);
      } else {
        callback(err, data);
      }
    });
  }



  var post_database = {
    getPosts: get_post,
  };

module.exports = post_database;
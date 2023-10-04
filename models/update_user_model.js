var AWS = require('aws-sdk');
AWS.config.update({region:'us-east-1'});
var db = new AWS.DynamoDB();

var updateAttribute = function(username, attribute, value, callback){
    var params = {
        TableName: 'users',
        Key: {
            'username': {S: username}
        },
        UpdateExpression: 'set ' + attribute + ' = :a',
        ExpressionAttributeValues: {
            ':a': {S: value}
        }
    };
    db.updateItem(params, function(err, data){
        if(err){
            callback(err,null);
        } else {
            callback(err,data);
        }
    });
}

var updateActive = function(username, value, callback){
    var params = {
        TableName: 'users',
        Key: {
            'username': {S: username}
        },
        UpdateExpression: 'SET #fieldToUpdate = :online',
        ExpressionAttributeNames: {
            "#fieldToUpdate": "online",
        },
        ExpressionAttributeValues: {
            ':online': {
                BOOL: value
            }
        }
    };
    db.updateItem(params, function(err, data){
        if(err){
            callback(err,null);
        } else {
            callback(err,data);
        }
    });
}

var update_user_model = { 
    update_attribute: updateAttribute,
    update_active: updateActive,
  };
  
  module.exports = update_user_model;
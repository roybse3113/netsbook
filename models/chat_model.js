var AWS = require('aws-sdk');
AWS.config.update({region:'us-east-1'});
var db = new AWS.DynamoDB();

var addRoomsToUserTable = function(room_id, users_list, callback){
  var roomList = [{"N" : String(room_id)}];
  var paramsArray = [];
  for (let i = 0; i < users_list.length; i++)  {
    var params = {
      TableName: "users",
      Key: {
        username: { S: users_list[i] } 
      },
      ExpressionAttributeValues: {
        ":val": {
          L: roomList
        },
        ":empty": {
          L: []
        }
      },
      ExpressionAttributeNames: {
        "#fieldToUpdate": "rooms",
      },
      UpdateExpression: "set #fieldToUpdate = list_append(if_not_exists(#fieldToUpdate, :empty), :val)",
    };
    paramsArray.push(params);
  }

  const putItemPromises = [];

  paramsArray.forEach(params => {
    putItemPromises.push(db.updateItem(params).promise());
  })
  Promise.all(putItemPromises).then( data =>  {
    callback(null, data);
  }).catch(err => {
    callback(err, "Failed: chat_model: addRoomstoUserTable");
  });
  // .catch( () => {
  //   //callback(err, "Failed");
  //   console.log("Failed");
  // })
  // need to figure out how to do wait for all promises to return, thats all
  
//   async function addItemsToDatabase() {
//     // Use the await keyword to wait until all putItem() functions have completed
//     await Promise.all(putItemPromises);
  
//     // All putItem() functions have completed, so do something here
//     callback(err, "Success: chat_model: addRoomsToUserTable");
//   }
//   // Call the async function
//   addItemsToDatabase().catch(error => {
//   // One or more putItem() functions failed, so handle the error here
//     callback(err, "Failed: chat_model: addRoomsToUserTable");
// });

}

var get_user = function(username, callback) {
    var params = {
      KeyConditions: {
        "username": {
          ComparisonOperator: 'EQ',
          AttributeValueList: [ { S: username } ]
        },
      },
      TableName: "users",
    };
  
    // return all matching users
    db.query(params, function(err, data) {
      if (err || data.Items.length === 0) {
        callback(err, null);
      } else {
        callback(err, data);
      }
    });
  }


  var inviteToNewChat = function(room_id, requester, invited, callback){
    var roomList = [{"N" : String(room_id)}];
    var params = {
      TableName: "users",
      Key: {
        username: { S: requester } 
      },
      ExpressionAttributeValues: {
        ":val": {
          L: roomList
        },
        ":empty": {
          L: []
        }
      },
      ExpressionAttributeNames: {
        "#fieldToUpdate": "rooms",
      },
      UpdateExpression: "set #fieldToUpdate = list_append(if_not_exists(#fieldToUpdate, :empty), :val)",
    };

    var params2 = {
        TableName: "users",
        Key: {
          username: { S: invited } 
        },
        ExpressionAttributeValues: {
          ":val": { L: roomList },
          ":empty": { L: [] }
        },
        ExpressionAttributeNames: {
          "#fieldToUpdate": "rooms",
        },
        UpdateExpression: "set #fieldToUpdate = list_append(if_not_exists(#fieldToUpdate, :empty), :val)",
      };
      db.updateItem(params, function(err, data) {
        if (err) {
          console.log(err);
          callback(err, null);
        }
        else{
          db.updateItem(params2, function(err2,data2){
              if (err){
                  console.log(err2);
                  callback(err2, null);
              } 
              else {
                  // if both update rooms works, then callback with requester's update data
                  callback(err, room_id);
              }
          })
        }
      });
  }

  var inviteToExistingRoom = function(room_id, invited, callback){
    var roomList = [{"N" : String(room_id)}];
    var params = {
        TableName: "users",
        Key: {
          username: { S: invited } 
        },
        ExpressionAttributeValues: {
          ":val": { L: roomList },
          ":empty": { L: [] }
        },
        ExpressionAttributeNames: {
          "#fieldToUpdate": "rooms",
        },
        UpdateExpression: "set #fieldToUpdate = list_append(if_not_exists(#fieldToUpdate, :empty), :val)",
      };
      db.updateItem(params, function(err, data) {
        if (err) {
          callback(err, null);
        }
        else{
          callback(err, data);
        }
      });
  }

  var updateUsersRoomTable = function(room_id, userToAdd, callback){
    var userList = [{"S" : userToAdd}]
    var params = {
      TableName: "rooms",
      Key: {
        room_id: { N: room_id } 
      },
      ExpressionAttributeValues: {
        ":val": {
          L: userList
        },
        ":empty": {
          L: []
        }
      },
      ExpressionAttributeNames: {
        "#fieldToUpdate": "users_list",
      },
      UpdateExpression: "set #fieldToUpdate = list_append(if_not_exists(#fieldToUpdate, :empty), :val)"
    };

    db.updateItem(params, function(err, data){
      if (err){
          console.log(err);
          callback(err, null);
      } 
      else {
          callback(err, data);
      }
  })
  }

  var putRoomTable = function(room_id, users, callback){
    // users is the list of users in the room
    var params = {
      TableName: "rooms",
      Item: {
        "room_id" : {N: String(room_id)},
        "users_list" : {L : users},
        "messages_list" : {L: []}
      }
    };
    db.putItem(params, function(err, data){
      if (err){
        console.log(err);
        callback(err, "Error: chat_model: putRoomTable");
      } else {
        console.log("putRoomTable Room iD " + room_id);
        callback(err, room_id);
      }
    })
  }
  
  var joinChat = function(roomID, callback){
    var params = {
      TableName: "rooms",
      KeyConditions: {
        "room_id": {
          ComparisonOperator: 'EQ',
          AttributeValueList: [ { N: String(roomID) } ]
        }
      }
    }

    db.query(params, function(err,data){
      if (err){
        console.log("ChatModel: get Chat room error");
        console.log(err);
        callback(err, null);
      } else {
        callback(err, data);
      }
    })
  }

  var getChatRooms = function(username, callback){
    var params = {
      TableName: "users",
      Item: {
        "username" : {S: username},
      }
    };
    db.query(params, function(err,data){
      if (err){
        console.log("ChatModel: getChatRooms Error");
        callback(err, null);
      } else {
        callback(err, data);
      }
    })
  }

  var addMSGTable = function(chat_id, text, sender, time, callback){
    var params = {
      TableName: "messages",
      Item: {
        "msg_id" : {N: String(chat_id)},
        "text" : {S : text},
        "sender" : {S: sender},
        "time" : {S: time}
      }
    };
    db.putItem(params, function(err,data){
      if (err){
        callback(err, null);
      } else {
        callback(err, data);
      }
    })
  }
  
  var addMSGUserTable = function(username, msg_id, callback){
      var msgList = [{"N" : String(msg_id)}]
      var params = {
        TableName: "users",
        Key: {
          username: { S: username } 
        },
        ExpressionAttributeValues: {
          ":val": {
            L: msgList
          },
          ":empty": {
            L: []
          }
        },
        ExpressionAttributeNames: {
          "#fieldToUpdate": "messages_list",
        },
        UpdateExpression: "set #fieldToUpdate = list_append(if_not_exists(#fieldToUpdate, :empty), :val)",
      };
      
  }

  var addMSGRoomTable = function(room_id, msg_id, callback){
    var msgList = [{"N" : String(msg_id)}]
    var params = {
      TableName: "rooms",
      Key: {
        room_id: { N: room_id } 
      },
      ExpressionAttributeValues: {
        ":val": {
          L: msgList
        },
        ":empty": {
          L: []
        }
      },
      ExpressionAttributeNames: {
        "#fieldToUpdate": "messages_list",
      },
      UpdateExpression: "set #fieldToUpdate = list_append(if_not_exists(#fieldToUpdate, :empty), :val)",
    };
    db.updateItem(params, function(err, data) {
      if (err) {
        console.log(err);
        callback(err, null);
      }
      else{
        callback(err,data);
      }
    })
  }

  var batchGetMSG = function(msgList, callback){
    var batchGetParams = {
      RequestItems: {
        "messages": {
          Keys: msgList
        }
      }
    }
    db.batchGetItem(batchGetParams, function(err,data){
      if (err) callback(err, null)
      else callback(err, data)
    })
}

  var getMSG =  function(msg_id, callback){
    var params = {
      KeyConditions: {
        "msg_id": {
          ComparisonOperator: 'EQ',
          AttributeValueList: [ { N: String(msg_id) } ]
        },
      },
      TableName: "messages",
    };
    db.query(params, function(err,data){
      if (err) callback(err, null)
      else {
        callback(err, data);
      }
    })
  }

  var getRooms = function(roomIDParamsArray, callback){
    var batchGetParams = {
      RequestItems: {
        "rooms": {
          Keys: roomIDParamsArray
        }
      }
    }
    db.batchGetItem(batchGetParams, function(err,data){
      if (err) callback(err, null)
      else callback(err, data)
    })
  }

  var chat_model = { 
    invite_to_new_chat: inviteToNewChat,
    put_roomTable: putRoomTable,
    invite_to_existing_room: inviteToExistingRoom,
    update_users_roomTable: updateUsersRoomTable,
    join_chat: joinChat,
    get_chat_rooms: getChatRooms,
    add_msg_table: addMSGTable,
    add_msg_user_table: addMSGUserTable,
    add_msg_room_table: addMSGRoomTable,
    get_msg: getMSG,
    batch_get_msg: batchGetMSG,
    get_rooms: getRooms,
    add_rooms_to_user_table: addRoomsToUserTable

  };
  
  module.exports = chat_model;
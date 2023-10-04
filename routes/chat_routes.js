var db = require('../models/database.js');
var chatdb = require('../models/chat_model.js');
// Lightsail name type here
const { Lightsail } = require('aws-sdk');
const e = require('express');

var createGroupChat = function(req, res){
    // console.log(JSON.stringify(req.body.users_list));
    var currUser = req.session.username;
    var arrString = req.body.users_list;
    //type array
    var users_list = arrString.split(",");
    console.log("chat_routes users_list: " + users_list);
    console.log("Users List in chat_routes createGroupChat: \n" + users_list);
    var room_id = Date.now();
    // this not yet working, need to fix promise array handling
    chatdb.add_rooms_to_user_table(room_id, users_list, function(err,data){
        if (err){
            console.log("Failed: chat_routes: add_rooms_to_user_table");
            console.log(err);
        } else {
            console.log("Success: chat_routes add_rooms_to_user_table");
            var users_list_params = [];
            for (const element of users_list) {
                users_list_params.push({"S" : element});
            }
            chatdb.put_roomTable(room_id, users_list_params, function(err,data){
                if (err){
                    console.log("Failed: chat_routes: put_roomTable ");
                    console.log(err);
                } else {
                    db.getUser(currUser, function(err, data){
                        if (err) {
                            console.log(data);
                            console.log(err);
                        } else {
                            var user = data.Items[0];
                            var friendsList = user.friends.L;
                            var profilePicture = user.profilePicture.S;
                            res.render("chat_room.ejs", {success: true, room_id: room_id, userList: users_list_params,
                                 messagesList: [], username: currUser, friendsList : friendsList, profilePicture});
                        }
                    })
                }
            })
        }
    })
}

var create2PersonChat = function(req, res){
    var requester = req.session.username;
    var invited = req.body.invited;
    var room_id = Date.now();
    chatdb.invite_to_new_chat(room_id, requester, invited, function(err, data1) {
        if (err) console.log("invite to chat failed");
        else {
            // put to rooms table
            var userList = [];
            userList.push({"S": requester});
            userList.push({"S": invited});
            console.log(userList);
            chatdb.put_roomTable(room_id, userList, function(err, unusedData){
                if (err) {
                    console.log(err);
                } else{
                    db.getUser(requester, function(err, data){
                        var user = data.Items[0];
                        var friendsList = user.friends.L;
                        var profilePicture = user.profilePicture.S;
                        res.render("chat_room.ejs", {success: true, room_id: room_id, userList: userList,
                             messagesList: [], username: requester, friendsList : friendsList, profilePicture})
                    })
                   
                }
            })
           
        }
    })
}

var inviteUserCurrRoom = function(req,res){
    var requester = req.session.username;
    console.log(req);
    var invited = req.body.invited;
    var room_id = req.body.roomID;
    chatdb.invite_to_existing_room(room_id, invited, function(err,data){
        if (err){
            console.log("Failed: chat_routes inviteUserCurrRoom: invite to existing chat room failed");
            console.log(err);
        } else {
            console.log("Success: invite to existing chat room");
            chatdb.update_users_roomTable(room_id, invited, function(err,data1){
                if (err){
                    console.log("Failed: Update Room Table with new User failed");
                    console.log(err);
                } else {
                    console.log("Success: Update Room Table with new User");
                    res.send({success_inviteToCurrRoom: true});
                }
            })
        }
    })

}

// get Chat Rooms should return list of rooms and list of sets of users?
// rooms for html link, sets of users so user knows whos in the room before clicking
var getChatRooms = function(req, res){
    // get username from session
    var username = req.session.username;
    if (username) {
        db.getUser(username, function(err,data){
            if (err){
                console.log(err);
                res.render("chat.ejs", {rooms: null, profilePicture: null});
            } else {
                // process list of chat rooms
                var user = data.Items[0];
                var friendsList = user.friends.L;
                var rooms = user.rooms.L;
                var profilePicture = user.profilePicture.S;
                if (rooms.length == 0){
                     res.render("chat.ejs", {rooms: [], friends: friendsList, username: username,
                         profilePicture});
                } else {
                    var roomSet = new Set();
                    rooms.forEach(roomObj =>
                        roomSet.add(String(roomObj.N)))
                    var roomIDParamsArray = []
                    roomSet.forEach(roomObj => {
                        var paramsObj = {"room_id" : {N: roomObj}};
                        roomIDParamsArray.push(paramsObj);
                    })
                    chatdb.get_rooms(roomIDParamsArray, function(err,data){
                        if (err){
                            console.log("chat_routes: getUser: get_rooms failed");
                            console.log(err);
                        } else {
                            console.log("Get Chat Rooms Success");
                            var outputRoomObjectArray = data.Responses.rooms;
                            console.log("outputRoomObjectArray" + JSON.stringify(outputRoomObjectArray));
                            res.render("chat.ejs", {rooms: outputRoomObjectArray, friends: friendsList, username: username,
                                 profilePicture});
                        }
                    })
                }
            }
        })
    } else {
        res.render('login.ejs', {message: "Log in first!"});
    }
}

var get_joinChatRoom = function(req,res) {
    var username = req.session.username;
    console.log("chat_routes.js: joinChatRoom req.query: " + JSON.stringify(req.query));
    console.log("chat_routes.js: joinChatRoom roomID: " + req.body.roomID);
    var roomID = req.query.roomID;
    chatdb.join_chat(roomID, function(err, data){
        if (err) console.log("chat_routes: joinChatRoom Error")
        else {
            console.log(JSON.stringify(data));
            data = data.Items[0];
            var userList = data.users_list.L;
            var messagesList = data.messages_list.L;
            db.getUser(username, function(err,data){
                if (err) console.log("Failed: getUser failed");
                else {
                    console.log("chat_routes: get_user success");
                    var user = data.Items[0];
                    var friendsList = user.friends.L;
                    var profilePicture = user.profilePicture.S;
                    console.log("MessagesList: " + messagesList);
                    //console.log(JSON.stringify(sortedMessagesListObject));
                    if (messagesList.length == 0){

                        res.render("chat_room.ejs", 
                        {success: true, room_id: roomID, userList: userList, 
                        messagesList: [], username: username,
                        friendsList: friendsList, profilePicture});
                    } else {
                        var msgSet = new Set();
                        messagesList.forEach(msgObj =>
                            msgSet.add(String(msgObj.N)))
                        var msgIDParamsArray = []
                        msgSet.forEach(msgObj => {
                            var paramsObj = {"msg_id" : {N: msgObj}};
                            msgIDParamsArray.push(paramsObj);
                        })
                        console.log("messagesList from room query: " + messagesList);
                        console.log("1st item: " + JSON.stringify(messagesList[0]));
                        var messagesListObject = [];
                        chatdb.batch_get_msg(msgIDParamsArray, function(err, data){
                            if (err){
                                console.log("Batch Get Messages Failed");
                                console.log(err);
                            } else {
                                console.log("Batch Get Messages Success");
                                messagesListObject = data.Responses.messages;
                                msg_idList = [];
                                messagesListObject.forEach(item => {
                                    msg_idList.push(item.msg_id.N);
                                })
                                msg_idList.sort(function(a,b) {
                                    return (a-b);
                                });
                                var sortedMessagesListObject = [];
                                msg_idList.forEach(id => {
                                    for (let i = 0; i < msg_idList.length; i++){
                                        if (messagesListObject[i].msg_id.N == id) {
                                            sortedMessagesListObject.push(messagesListObject[i]);
                                        }
                                    }
                                })
                                res.render("chat_room.ejs", 
                                {success: true, room_id: roomID, userList: userList, 
                                messagesList: sortedMessagesListObject, username: username,
                                friendsList: friendsList, profilePicture});
                            }
                        })
                        // end of getUser else 
                    }
                }
            })

        }
    })
}

var joinChatRoom = function(req,res) {
    var username = req.session.username;
    console.log("chat_routes.js: joinChatRoom req.body: " + JSON.stringify(req.body));
    console.log("chat_routes.js: joinChatRoom roomID: " + req.body.roomID);
    var roomID = req.body.roomID;
    chatdb.join_chat(roomID, function(err, data){
        if (err) console.log("chat_routes: joinChatRoom Error")
        else {
            console.log(JSON.stringify(data));
            data = data.Items[0];
            var userList = data.users_list.L;
            var messagesList = data.messages_list.L;
            db.getUser(username, function(err,data){
                if (err) console.log("Failed: getUser failed");
                else {
                    console.log("chat_routes: get_user success");
                    var user = data.Items[0];
                    var friendsList = user.friends.L;
                    var profilePicture = user.profilePicture.S;
                    console.log("MessagesList: " + messagesList);
                    //console.log(JSON.stringify(sortedMessagesListObject));
                    if (messagesList.length == 0){

                        res.render("chat_room.ejs", 
                        {success: true, room_id: roomID, userList: userList, 
                        messagesList: [], username: username,
                        friendsList: friendsList, profilePicture});
                    } else {
                        var msgSet = new Set();
                        messagesList.forEach(msgObj =>
                            msgSet.add(String(msgObj.N)))
                        var msgIDParamsArray = []
                        msgSet.forEach(msgObj => {
                            var paramsObj = {"msg_id" : {N: msgObj}};
                            msgIDParamsArray.push(paramsObj);
                        })
                        console.log("messagesList from room query: " + messagesList);
                        console.log("1st item: " + JSON.stringify(messagesList[0]));
                        var messagesListObject = [];
                        chatdb.batch_get_msg(msgIDParamsArray, function(err, data){
                            if (err){
                                console.log("Batch Get Messages Failed");
                                console.log(err);
                            } else {
                                console.log("Batch Get Messages Success");
                                messagesListObject = data.Responses.messages;
                                msg_idList = [];
                                messagesListObject.forEach(item => {
                                    msg_idList.push(item.msg_id.N);
                                })
                                msg_idList.sort(function(a,b) {
                                    return (a-b);
                                });
                                var sortedMessagesListObject = [];
                                msg_idList.forEach(id => {
                                    for (let i = 0; i < msg_idList.length; i++){
                                        if (messagesListObject[i].msg_id.N == id) {
                                            sortedMessagesListObject.push(messagesListObject[i]);
                                        }
                                    }
                                })
                                res.render("chat_room.ejs", 
                                {success: true, room_id: roomID, userList: userList, 
                                messagesList: sortedMessagesListObject, username: username,
                                friendsList: friendsList, profilePicture});
                            }
                        })
                        // end of getUser else 
                    }
                }
            })

        }
    })
}


var addMSGTable = function(req, res){
    var chat_id = req.body.chat_id;
    var text = req.body.text;
    var sender = req.body.sender;
    // Time?
    var time = "1";

    chatdb.add_msg_table(chat_id, text, sender, time, function(err,data){
        if (err){
            console.log("chat_routes.js: addMSGTable Error");
            console.log(err);

        } else {
            console.log("chat_routes.js: addMSGTable Success");
            res.send({addMSGTable: true});
        }
    })
}

var addMSGUserTable = function(req, res){
    var username = req.session.username;
    var chat_id = req.body.chat_id;
    
}

var addMSGRoomTable = function(req, res){
    // roomID or room_id?
    var room_id = req.body.room_id;
    var chat_id = req.body.chat_id;
    chatdb.add_msg_room_table(room_id, chat_id, function(err, data){
        if(err){
            console.log("chat_routes.js: addMSGRoomTable Error");
            console.log(err);
        } else {
            console.log("chat_routes.js: addMSGRoomTable Success");
            res.send({addMSGRoomTable: true});
        }
    })
}




var chat_routes = { 
    create_2_person_chat: create2PersonChat,
    get_chat_rooms: getChatRooms,
    join_chat: joinChatRoom,
    add_msg_msg_table: addMSGTable,
    add_msg_user_table: addMSGUserTable,
    add_msg_room_table: addMSGRoomTable,
    invite_user_curr_room: inviteUserCurrRoom,
    create_groupchat: createGroupChat,
    get_join_chat_room: get_joinChatRoom
  };
  
  module.exports = chat_routes;
/* Some initialization boilerplate. Also, we include the code from
   routes/routes.js, so we can have access to the routes. Note that
   we get back the object that is defined at the end of routes.js,
   and that we use the fields of that object (e.g., routes.get_main)
   to access the routes. */

var express = require('express');
var routes = require('./routes/routes.js');
var chat_routes = require('./routes/chat_routes.js');
var profile_routes = require('./routes/profile_routes.js');
var update_user_routes = require('./routes/update_user_routes.js');
var userdb = require('./models/update_user_model.js');
var session = require('express-session')
//var app = express();
var app = require('express')();
var server = require('http').createServer(app);
const io = require('socket.io')(server);
app.use(express.static('public'))
app.use(express.urlencoded());
app.use(session({
   secret: 'loginSecret',
   cookie: {
      // 10 minute sessions
      maxAge: 1000 * 60 * 10
   }
}));

server.listen(8080);
//app.listen(8080);
console.log('Server running on port 8080. Now open http://localhost:8080/ in your browser!');

// for chat_routes.js: createGroupChat
const bodyParser = require('body-parser');
app.use(bodyParser.json());


io.on("connection", function(socket){
   socket.on('online', function(username){
      userdb.update_active(username, true, function(err, data) {
         if (err) {
               console.log("Error setting online to true: " + err);
            //  res.send({success: false, message: 'Error updating affiliation', username: username});
         } 
      });

      socket.on('disconnect', function(){
         // socket.disconnect();
         userdb.update_active(username, false, function(err, data) {
            if (err) {
                console.log("Error updating online to false: " + err);
                //res.send({success: false, message: 'Error updating affiliation', username: username});
            } 
         });
      });

   });

      // Set user's active status to false when they disconnect
   

   socket.on("chat message", function(obj){
      console.log("App.js Socket: " + obj);
      console.log("App.js Socket obj.room: " + obj.room);
      //io.emit("chat message", obj);
      io.to(obj.room).emit("chat message", obj);
   })
   // add invite chat room socket function
   // need to implement joining a specific room when a chat is created
   // so that messages are sent to a certain room and not all chat rooms
   socket.on("join", function(obj){
    console.log("Server: Join Room socket received, room: " + obj.room);
    socket.join(obj.room);
   })
   socket.on("leave", function(obj){
    console.log("Server: Leave Room socket received")
    socket.leave(obj.room);
   })

   socket.on('invite user', function(obj){
    console.log("Server: invite user object received");
    console.log("Server should send invite user socket message to " + obj.invited);
    io.to(obj.room).emit("invite user", obj);
    var invitedUser = obj.invited;
    io.to(invitedUser).emit('invite user', obj);
   })
   
   socket.on('currUser', function(obj){
    console.log("Server: currUser socket message received");
    socket.join(obj.currUser);
   })
})

// Socket route functions


/* Below we install the routes. The first argument is the URL that we
   are routing, and the second argument is the handler function that
   should be invoked when someone opens that URL. Note the difference
   between app.get and app.post; normal web requests are GETs, but
   POST is often used when submitting web forms ('method="post"'). */

// app.get('/', routes.get_main);
app.get('/', routes.get_login);
app.post('/checklogin', routes.check_login);
app.get('/signup', routes.sign_up);
app.post('/createaccount', routes.create_account);
app.get('/logout', routes.log_out);
app.get('/home', routes.get_home);
app.put('/addFriend', routes.add_friend);
app.put('/deleteFriend', routes.delete_friend);
app.put('/updateInterest', routes.update_interest);
app.put('/addComment', routes.add_comment);
app.get('/posts', routes.get_posts);

app.post('/searchName', routes.search_name);

// news routes
app.get('/news', routes.get_news);
app.put('/likeArticle', routes.like_article);
app.get('/searchNews', routes.search_news);
// app.get('/articles', routes.load_articles);

// add the list result of getFriends to home.ejs and chat.ejs?
app.get('/getFriends', routes.get_friends); // WHAT DOES THIS DO?
app.get('/chat', chat_routes.get_chat_rooms)
app.post('/createChat', chat_routes.create_2_person_chat);
app.get('/joinChat2', chat_routes.get_join_chat_room);
app.post('/joinChat', chat_routes.join_chat);
app.post('/addMSGTable', chat_routes.add_msg_msg_table);
app.post('/addMSGUserTable', chat_routes.add_msg_user_table);
app.post('/addMSGRoomTable', chat_routes.add_msg_room_table);
app.post('/inviteUserToCurrRoom', chat_routes.invite_user_curr_room);
app.post('/createGroupChat', chat_routes.create_groupchat);
app.get('/toUpdateUser', update_user_routes.to_update_user);

// update user routes
app.put('/updateAffiliation', update_user_routes.update_affiliation);
app.put('/updateEmail', update_user_routes.update_email);
app.put('/updatePassword', update_user_routes.update_password);
app.put('/updateInterests', update_user_routes.update_interests);
app.put('/updateProfilePicture', update_user_routes.update_profile_picture);

//profile routes
app.get('/profilePage', profile_routes.to_profile);
app.get('/toOtherProfile', profile_routes.to_other_profile);
app.put('/createPost', profile_routes.create_post);
app.put('/createStatusUpdate', profile_routes.create_status_update);


// visualizer routes
app.get('/visualizer', routes.visualizer);
app.get('/friendVisualizer', routes.friendVisualizer);
app.get('/getFriends/:user', routes.getVisualizerFriends);

/* Run the server */





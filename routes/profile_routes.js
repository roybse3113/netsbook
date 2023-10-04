var db = require('../models/database.js');
var userDB = require('../models/chat_model.js');
var postDB = require('../models/post_database.js');

var toProfile = function(req, res) {
    var username = req.session.username;
    if (username) {
        db.getUser(username, function(err, data){
          if(err){
              console.log("Error getting user");
              res.render('profile_page.ejs', {message: 'Error getting user'});
          } else {
              var userObj = data.Items[0];
              var username = userObj.username.S;
              var interests = userObj.interests.L;
              var affiliation = userObj.affiliation.S;
              var email = userObj.email.S;
              var posts = userObj.posts.L;
              var listOfPosts = [];
            if (posts.length > 0) {
              posts.forEach(function(post) {
                  postDB.getPosts(post.N, function(err, data){
                      if(err){
                          res.render('profile_page.ejs', {message: err});
                      } else {
                          var postObj = data.Items[0];
                          listOfPosts.push(postObj);
                          if (listOfPosts.length == posts.length) {
                            listOfPosts.sort(function(a, b){return b.ID.N - a.ID.N});
                              res.render('profile_page.ejs', {message: null, result: {user: userObj, posts: listOfPosts, ownProfile: true}, username });
                          }
                      }
                  });
              });
          } else {
              res.render('profile_page.ejs', {message: null, result: {user: userObj, posts: listOfPosts, ownProfile: true}, username });
          }
          }
      });
    } else {
      res.render('login.ejs', {message: "Log in first!"});
    }
}

var toOtherProfile = function(req, res) {
    var username = req.query.param;
    if (username) {
        db.getUser(username, function(err, data){
          if(err){
              console.log("Error getting user");
              res.render('profile_page.ejs', {message: 'Error getting user'});
          } else {
              var userObj = data.Items[0];
              var username = userObj.username.S;
              var posts = userObj.posts.L;
              var listOfPosts = [];
              if (posts.length > 0) {
                  posts.forEach(function(post) {
                      postDB.getPosts(post.N, function(err, data){
                          if(err){
                              res.render('profile_page.ejs', {message: err});
                          } else {
                              var postObj = data.Items[0];
                              listOfPosts.push(postObj);
                              if (listOfPosts.length == posts.length) {
                                listOfPosts.sort(function(a, b){return b.ID.N - a.ID.N});
                                var friendlist = [];
                                userObj.friends.L.forEach(function(friend) {
                                    friendlist.push(friend.S);
                                })
                                  if (friendlist.includes(req.session.username)) {
                                    res.render('profile_page.ejs', {message: null, result: {user: userObj, posts: listOfPosts, ownProfile: false, isFriends: true,}, username });
                                  } else {
                                    res.render('profile_page.ejs', {message: null, result: {user: userObj, posts: listOfPosts, ownProfile: false, isFriends: false,}, username });
                                  }

                              }
                          }
                      });
                  });
              } else {
                  res.render('profile_page.ejs', {message: null, result: {user: userObj, posts: listOfPosts, ownProfile: false}, username });
              }
          }
      });
    } else {
      res.render('login.ejs', {message: "Log in first!"});
    }
}

var createPost = function(req, res) {
    var username = req.session.username, ID = req.body.ID, content = req.body.content, date = req.body.date;
    var toUser = req.body.toUser; 
    var postFields = {ID, content, date};
    db.createPost(username, "Post", toUser, postFields, function(err, data) {
      if (err) {
        return res.send({success: false, message: err, result: null});
      } else {
        // if post successfully created, add postID to user posts list
  
        db.addPost(username, "posts", ID, function(err, data) {
          if (err) {
            return res.send({success: false, message: err, result: null});
          } else {
            // successfully added post ID to posts list
            db.addPost(toUser, "posts", ID, function(err, data) {
                if (err) {
                    return res.send({success: false, message: err, result: null});
                } else {
                    console.log("Successfully created post");
                    return res.send({success: true, result: data});
                }
            });
          }
        });
      }
    });
  };

  var createStatusUpdate = function(req, res) {
    var username = req.session.username, ID = req.body.ID, content = req.body.content, date = req.body.date;
    var toUser = req.body.toUser; 
    var postFields = {ID, content, date};

    db.createPost(username, "Status Update", toUser, postFields, function(err, data) {
      if (err) {
        return res.send({success: false, message: err, result: null});
      } else {
        // if post successfully created, add postID to user posts list
  
        db.addPost(username, "posts", ID, function(err, data) {
          if (err) {
            return res.send({success: false, message: err, result: null});
          } else {
            console.log("Successfully status update post");
            return res.send({success: true, result: data});
          }
        });
      }
    });
  };



var profile_routes = { 
    to_profile: toProfile,
    to_other_profile: toOtherProfile,
    create_post: createPost,
    create_status_update: createStatusUpdate,
};


module.exports = profile_routes;
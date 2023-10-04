var db = require('../models/database.js');
var CryptoJS = require("crypto-js");
var postDB = require('../models/post_database.js');


var getLogin = function(req, res) {
  var username = req.session.username;
  if (username) {
    res.redirect('/home');
  } else {
    res.render('login.ejs', {message: null});
  }
};

var checkLogin = function(req, res) {
  // set the user and password accordingly
  var username = req.body.username.toLowerCase(), password = req.body.password;
  //check the login
  db.checklogin(username, function(err, data) {
    if (err) {
      res.render('login.ejs', {message: 'Incorrect or invalid username or password'});
    } else if (data) {
      var hash = CryptoJS.SHA256(password).toString();
      if (data === hash) {
        // update online status
        db.updateOnline(username, "online", true, function(err, data) {
          if (err) res.render('login.ejs', {message: 'Failed to log in. Please try again.'});
          else {
            console.log('Successfully logging in');
            // cookie/session for logged in user
            req.session.username = username;
            req.session.password = password;
            res.redirect('/home');
          }
        });
      } else {
        // password doesn't match
        res.render('login.ejs', {message: 'Incorrect password. Please try again.'});
      }
    } else {
      // no user in db found
      res.render('login.ejs', {message: 'No such user found'});
    }
  });
};

var signUp = function(req, res) {
  var username = req.session.username;
  if (username) {
    res.redirect('/home');
  } else {
    res.render('signup.ejs', {message: null});
  }
};

var updateAccount = function(req, res) {
  var username = req.body.username;
  // complete function to update fields of user
}

// route to display home content: users and posts
var getHomeContent = function(req, res) {
  var username = req.session.username;
  if (username) {
    db.getUser(username, function(err, dataUsers) {
      if (err) {
        res.render('home.ejs', {message: err, result: null, username });
      } else {
        // if data exists for users, now get posts
        console.log(dataUsers.Items[0].friends);
        // session user's friendlist
        var friendlist = dataUsers.Items[0].friends.L;
        var profilePicture = dataUsers.Items[0].profilePicture.S;
        // all of the user's friends stored as user objects
        var userlist = [];
        var postlist = new Set();
        if (friendlist.length == 0) {
          res.render('home.ejs', {message: null, result: { users: userlist, posts: postlist }, username,
          profilePicture});
          return;
        }
        friendlist.forEach(function(user) {
          db.getUser(user.S, function(err, data) {
            if (err) {
              res.render('home.ejs', {message: err, result: null, username, profilePicture});
            } else {
              userlist.push(data.Items[0]);
              if (userlist.length === friendlist.length) {
                db.getPosts(function(err, dataPosts) {
                  if (err) {
                    res.render('home.ejs', {message: err, result: null, username, profilePicture});
                  } else {
                    // if data exists for posts
                    friendlist.push(dataUsers.Items[0].username);
                    // get all posts
                    if (dataPosts.length == 0) {
                      res.render('home.ejs', {message: null, result: { users: userlist, posts: postlist }, username , profilePicture});
                      return;
                    }
                    // data posts is every post
                    dataPosts.forEach(function (post) {
                      // userlist contains all of the user's friends as object user
                      userlist.forEach(function (user) {
                        var friendsPosts = user.posts.L;
                        // each one is an ID
                        friendsPosts.forEach(function (friendPost) {
                          if (friendPost.N === post.ID.N) {
                            postlist.add(post);
                          }
                        });
                        var userPosts = dataUsers.Items[0].posts.L;
                        userPosts.forEach(function (userPost) {
                          if (userPost.N === post.ID.N) {
                            postlist.add(post);
                          }
                        });
                      });
                      
                    });
                    postlist = Array.from(postlist);
                    postlist.sort(function(a, b){return b.ID.N - a.ID.N});
                    userlist.sort(function(a, b){return a.username.S.localeCompare(b.username.S)});
                    res.render('home.ejs', {message: null, result: { users: userlist, posts: postlist }, username,
                    profilePicture });
                    //return res.send({result: { users: dataUsers, posts: dataPosts }})
                  }
                });
              }
            }
          });
        });
      } 
    });
  } else {
    res.render('login.ejs', {message: "Log in first!"});
  }
}

var ajaxGetHomeContent = function(req, res) {
  var username = req.session.username;
  db.getUser(username, function(err, dataUsers) {
    if (err) {
      res.render('home.ejs', {message: err, result: null, username });
    } else {
      var friendlist = dataUsers.friendlist;
      // if data exists for users, now get posts
      db.getPosts(function(err, dataPosts) {
        if (err) {
          res.render('home.ejs', {message: err, result: null, username });
        } else {
          // if data exists for posts
          //res.render('home.ejs', {message: null, result: { users: dataUsers, posts: dataPosts }, username });
          return res.send({result: { users: friendlist, posts: dataPosts }})
        }
      });
    } 
  });
}

var addFriend = function(req, res) {
  // need to add recipient and requester of friendship to each other's friends lists
  // need to add logic of accepting/declining friend requests 
  
  // recipient of friend request
  var recipient = req.body.username;
  // requester / currently logged in user
  var requester = req.session.username;

  // if we need to store friends as list of objects
  // ----------------------------------------------
  // db.getUser(username, function(err, data) {
  //   if (err) {
  //     return res.send({success: false, message: err, result: null});
  //   } else if (data) {
  //     console.log("user: ", data.Items[0]);
  //     var userToAdd = data.Items[0];
  //     // if there was a user with same username found, add as friend
  //     db.addFriend(req.session.username, "friends", userToAdd, function(err, data) {
  //       if (err) return res.send({success: false, message: err, result: null});
  //       else return res.send({success: true, result: data});
  //     });
  //   } else {
  //     // if there was no user with same username found, cannot add a friend
  //     return res.send({success: false, message: err, result: null});
  //   }
  // });

  // add recipient to requester's friends list
  // --------------------------------
  // check if user is already in friends list
  db.getUser(requester, function(err, data) {
    if (data) {
      // if user is found
      var user = data.Items[0];
      var seenFriends = new Set();
      // set of friends list
      user.friends.L.forEach(friend => seenFriends.add(friend.S));
      if (!seenFriends.has(recipient)) {
        // only add friend if user is not already in friends list
        db.addFriend(requester, "friends", recipient, function(err, data) {
          if (err) return res.send({success: false, message: err, result: null});
          else {
            // add requester to recipient's friends list
            // --------------------------------
            // check if user is already in friends list
            db.getUser(recipient, function(err, data) {
              if (data) {
                // if user is found
                var user = data.Items[0];
                var seenFriends = new Set();
                // set of friends list
                user.friends.L.forEach(friend => seenFriends.add(friend.S));
                if (!seenFriends.has(requester)) {
                  // only add friend if user is not already in friends list
                  db.addFriend(recipient, "friends", requester, function(err, data) {
                    if (err) return res.send({success: false, message: err, result: null});
                    else {
                      var ID = (new Date()).getTime().toString();
                      var content = recipient + " is now friends with " + requester;
                      var date =(new Date()).toLocaleString();
                      var postFields = {ID, content, date};

                      db.createPost(recipient, "New Friendship", recipient, postFields, function(err, data) {
                        if (err) {
                            console.log(err);
                            return res.send({success: false, message: err, result: null});
                        } else {        
                            db.addPost(recipient, "posts", ID, function(err, data) {
                                if (err) {
                                    console.log("ERROR ADDING IT TO USER POSTS");
                                    return res.send({success: false, message: err, result: null});
                                } else {
                                    
                                  var ID = (new Date()).getTime().toString();
                                  var content = requester + " is now friends with " + recipient;
                                  var date =(new Date()).toLocaleString();
                                  var postFields = {ID, content, date};

                                  db.createPost(requester, "New Friendship", requester, postFields, function(err, data) {
                                    if (err) {
                                        console.log(err);
                                        return res.send({success: false, message: err, result: null});
                                    } else {        
                                        db.addPost(requester, "posts", ID, function(err, data) {
                                            if (err) {
                                                console.log("ERROR ADDING IT TO USER POSTS");
                                                return res.send({success: false, message: err, result: null});
                                            } else {
                                                console.log("Successfully status update post");
                                                return res.send({success: true, result: data});
                                            }
                                        });
                                    }
                                });


                                }
                            });
                        }
                    });

                      // create post



                      
                    }
                  });
                }
              } else {
                // no such user found or error
                return res.send({success: false, message: err, result: null});
              }
            });
          }
        });
      }
    } else {
      // no such user found or error
      return res.send({success: false, message: err, result: null});
    }
  });
}


var deleteFriend = function(req, res) {
  // recipient of friend request
  var recipient = req.body.username;
  // requester / currently logged in user
  var requester = req.session.username;

  db.getUser(requester, function(err, data) {
    if (data) {
      // if user is found
      var user = data.Items[0];
      var seenFriends = [];
      // set of friends list
      user.friends.L.forEach(friend => seenFriends.push(friend.S));
      var index = seenFriends.indexOf(recipient);
      if (index != -1) {
        // only add friend if user is not already in friends list
        db.deleteFriend(requester, index, function(err, data) {
          if (err) return res.send({success: false, message: err, result: null});
          else {
            db.getUser(recipient, function(err, data) {
              if (data) {
                // if user is found
                var user = data.Items[0];
                var seenFriends = [];
                // set of friends list
                user.friends.L.forEach(friend => seenFriends.push(friend.S));
                var index = seenFriends.indexOf(requester);
                if (index != -1) {
                  // only add friend if user is not already in friends list
                  db.deleteFriend(recipient, index, function(err, data) {
                    if (err) return res.send({success: false, message: err, result: null});
                    else return res.send({success: true, result: data});
                  });
                } else {
					return res.send({success: false, message: "user is not in friend's list'", result: null});
				}
              } else {
                // no such user found or error
                return res.send({success: false, message: err, result: null});
              }
            });
          }
        });
      } else {
		// user is not in friend's list
	  	return res.send({success: false, message: "user is not in friend's list'", result: null});
	  }
    } else {
      // user is not found
      return res.send({success: false, message: err, result: null});
    }
  });
}

var updateInterest = function(req, res) {
  var username = req.session.username, interests = req.body.interests;
  console.log(interests);

  // update interests to those that are selected
  db.updateInterest(username, "interests", interests, function(err, data) {
    if (err) return res.send({success: false, message: err, result: null});
    else return res.send({success: true, result: data});
  });

  // add to table called interest_to_user
  


  // implementation logic for adding unique interests
  // ------------------------------------------------
  // // check if interest is already in interests list
  // db.getUser(req.session.username, function(err, data) {
  //   if (data) {
  //     // if user is found
  //     var user = data.Items[0];
  //     console.log(user.interests.L);
  //     var seenInterests = new Set();
  //     // set of interest already in list
  //     user.interests.L.forEach(interest => seenInterests.add(interest.S));
  //     interests = interests.filter(interest => !seenInterests.has(interest));
  //     console.log("HERE: ", interests)
  //     // only add interests not already in user interests
  //     db.addInterest(username, "interests", interests, function(err, data) {
  //       if (err) return res.send({success: false, message: err, result: null});
  //       else return res.send({success: true, result: data});
  //     });
  //   } else {
  //     // no such user found or error
  //     return res.send({success: false, message: err, result: null});
  //   }
  // });
}

var addComment = function(req, res) {
  var ID = req.body.ID, comment = req.body.comment;
  var username = req.session.username;
  console.log(ID, ": ", comment);
  db.addComment(username, ID, "comments", comment, function(err, data) {
    if (err) return res.send({success: false, message: err, result: null});
    else return res.send({success: true, result: data});
  });
}

var likeArticle = function(req, res) {
  var username = req.session.username;
  var link = req.body.link;
  db.addArticle(username, link, function(err, data) {
    if (err) return res.send({success: false, message: err, result: null});
    else {
      db.createArticleToUser(username, link, function(err, data) {
        if (err) return res.send({success: false, message: err, result: null});
        else return res.send({success: true, result: data});
      });
    }
  });
}

var createAccount = function(req, res) {
  console.log(req.body);
  var username = req.body.username.toLowerCase(), password = CryptoJS.SHA256(req.body.password).toString(), firstName = req.body.firstName, lastName = req.body.lastName, birthday = req.body.birthMonth + " " + req.body.birthDay + ", " + req.body.birthYear, email = req.body.email, affiliation = req.body.affiliation, interests = req.body.interests;
  // check if imageURL exists or not when creating userFields object
  var profilePicture = req.body.profilePicture;
  // check for the user with the inputted username
  // only proceed if at least 2 interests selected
  if (typeof(interests) === "string") { // means they only selected 1 interest
    res.render('signup.ejs', {message: "Minimum 2 interests required"});
  } else {
    // if user fills out all fields, first check if username exists
    db.getUser(username, function(err, data) {
      if (err) {
        res.render('signup.ejs', {message: "Error searching for user"});
      } else if (data) {
        // if there was a user with same username found
        res.render('signup.ejs', {message: "Username already taken."});
      } else {
        // if there was no user with same username found, create user account
        var userFields = {username, password, firstName, lastName, birthday, email, affiliation, interests};
        if (Boolean(profilePicture)){
          userFields.profilePicture = profilePicture;
        } else {
          userFields.profilePicture = "https://styles.redditmedia.com/t5_2s4l8/styles/communityIcon_2wyzqtna1ka31.png";
        }
        db.createaccount(userFields, function(err, data) {
          if (err) {
            console.log(err);
            res.render('signup.ejs', {message: 'Error creating account'});
          } else {
            // cookie/session for logged in user
            req.session.username = username;
            req.session.password = password;

            //if successfully created, accordingly add entries to interest_to_user table
            db.addInterestToUser(username, interests, function(err,data) {
              if (err) {
                res.render('signup.ejs', {message: err});
              } else {
                // redirect to home afterwards
                res.redirect('/home');
              }
            })
          } 
        });
      }
    });
  }
};

var createPost = function(req, res) {
  var username = req.session.username, ID = req.body.ID, content = req.body.content, date = req.body.date;
  var postFields = {ID, content, date};
  db.createPost(username, postFields, function(err, data) {
    if (err) {
      return res.send({success: false, message: err, result: null});
    } else {
      // if post successfully created, add postID to user posts list

      db.addPost(username, "posts", ID, function(err, data) {
        if (err) {
          console.log("unsuccessful in creating post");
          return res.send({success: false, message: err, result: null});
        } else {
          // successfully added post ID to posts list
          console.log("Successfully created post");
          return res.send({success: true, result: data});
        }
      });
    }
  });
};

var searchNews = function(req, res) {
  var keyword = req.query.keyword;
  db.searchNews(keyword, function(err, data) {
    if (err){
      console.log("SEARCH NEWS FAILED");
      res.send({
        success: false,
        data: null,
      });
    } 
    else {
      console.log("SEARCH NEWS SUCESS");
      console.log("DATA" + data);
      var freqMap = new Map();
      for (var i = 0; i < data.length; i++) {
        var url = data[i];
        if (freqMap.has(url)) {
          freqMap.set(url, freqMap.get(url) + 1);
        } else {
          freqMap.set(url, 1);
        }
      }
      const sortedMap = new Map(
        Array.from(freqMap.entries()).sort((a, b) => a[1] > b[1])
      )
      console.log(sortedMap);
      var articleArr = Array.from(sortedMap.entries()).slice(0,10);
      console.log("1st element in arr " + JSON.stringify(articleArr[0]));
      var listUrl = [];
      for (var i = 0; i < articleArr.length; i++) {
        db.getArticleByUrl(articleArr[i][0].link.S, function(err, data) {
          if (err) {
            res.render("articles_search.ejs", {data: null, result: null, username: req.session.username});
          } else {
            listUrl.push(data.Items[0]);
            if (listUrl.length === articleArr.length) {
              res.render("articles_search.ejs", {success: true, result: listUrl, username: req.session.username});
            }
          }
      })
}
    }
  });
}

var loadNewsFeed = function(req, res) {
  res.render("newsfeed.ejs", {username: req.session.username});
}

var getNewsFeed = function(req, res) {
  var username = req.session.username;
  if (username) {
    db.getRecommendedArticles(username, function(err, urls) {
      if (err) 
        res.render("newsfeed.ejs", {data: null, result: null, username: req.session.username});
      else {
        var listUrl = [];
        var urlList = urls.Items.sort(function(a, b){return a.weight.N - b.weight.N});
        urlList = urlList.slice(0,14);
        for (var i = 0; i < urlList.length; i++) {
          db.getArticleByUrl(urlList[i].url.S, function(err, data) {
            if (err) {
              res.render("newsfeed.ejs", {data: null, result: null, username: req.session.username});
            } else {
              listUrl.push(data.Items[0]);
              if (listUrl.length === urlList.length) {
                res.render("newsfeed.ejs", {success: true, result: listUrl, username: req.session.username});
              }
            }
          });
        }
        //res.render("newsfeed.ejs", {success: true, result: listUrl, username: req.session.username});

      }
    }); 
  } else {
    res.render('login.ejs', {message: "Log in first!"});
  }
};

var logOut = function(req, res) {
  // reset session credentials when logging out
  var username = req.session.username;
  if (username) {
    db.updateOnline(username, "online", false, function(err, data) {
      if (err) {
        res.redirect('/home');
      } else {
        console.log('Successfully logging out');
        req.session.username = null;
        req.session.password = null;
        req.session.fullname = null;
        // send back to log in page
        res.redirect('/');
      }
  });
  } else {
    res.render('login.ejs', {message: "Log in first!"});
  }
}

var searchName = function(req, res) {
	var text = req.body.text;
	db.searchName(text, function(err, data) {
		if (err) {
			return res.send({success: false, message: err, result: null});
		} else {
			return res.send({success: true, result: data});
		}
	});
}

var getFriends = function(req, res) {
  var username = res.session.username;
  db.getUser(username, function(err, data){
    if (err) {
      console.log(err);
    } else {
      var user = data.Items[0];
      friendsNames = user.friends.L;
      res.send(friendsName);
    }
  })
}

var getPosts = function(req, res) {
  db.getPosts(function(err, data){
    if (err) {
      console.log({success: false, err});
    } else {
      var posts = data.Items[0];
      res.send({success: true, posts});
    }
  })
}

var visualizer = function(req, res)
{
	if (req.session.username) {
    db.getUsers(function(err, data)
		{
      if (err) {
        console.log(err);
      } else {
        res.render('friendvisualizer.ejs', {users: data, user: req.session.username});
      }
		});
  } else {
		res.render('login.ejs', {message: "Log in first!"});
	}
}

var friendVisualizer = function(req, res) {
	var username = req.session.username;
	if (username) {
    db.getUser(username, function(err, data)
    {
      if (err) {
        console.log(err);
      } else {
        let user = data.Items[0];
        friendNames = user.friends.L;
        let friendsJSON = [];

        let promise = db.getFriendsInfo(friendNames);
        promise.then(function(friends) {
          for (let i = 0; i < friends.length; i++) {
            let friend = friends[i];
            let tempJson = {"id": friend[0].username.S, "name": friend[0].firstName.S + " " + friend[0].lastName.S, "data":{}, "children":[]};
            friendsJSON.push(tempJson);
          }
          let json = {"id": username, "name": user.firstName.S + " " + user.lastName.S, "data": {}, "children": friendsJSON};
          res.send(json);
        });
      }
    });
  } else {
    res.render('login.ejs', {message: "Log in first!"});
  }
};

var getVisualizerFriends = function(req, res){
  var username = req.params.user;
  db.getUser(req.session.username, function(err, data)
	{
    db.getUser(username, function(err, data2) {
      if (err) {
        console.log(err);
      } else {
        let user = data2.Items[0];
        friendsNames = user.friends.L;
        let friendsJSON = [];
  
        let promise = db.getFriendsInfo(friendsNames);
        promise.then(function(friends) {
          for (let i = 0; i < friends.length; i++) {
            let friend = friends[i];
            let tempJson = {"id": friend[0].username.S, "name": friend[0].firstName.S + " " + friend[0].lastName.S, "children":[]};
            if (data.Items[0].affiliation.S == friend[0].affiliation.S) {
                friendsJSON.push(tempJson);
            }
          }
          let json = {"id": username, "name": user.firstName.S + " " + user.lastName.S, "children": friendsJSON};
          res.send(json);
        });
      }
    })
	});
}


var routes = { 
  get_login: getLogin,
  check_login: checkLogin,
  sign_up: signUp,
  create_account: createAccount,
  log_out: logOut,
  get_home: getHomeContent,
  add_friend: addFriend,
  search_news: searchNews,
  delete_friend: deleteFriend,
  update_interest: updateInterest,
  add_comment: addComment,
  create_post: createPost,
  search_name: searchName,
  get_friends: getFriends,
  like_article: likeArticle,
  get_news: getNewsFeed,
  visualizer: visualizer,
	friendVisualizer: friendVisualizer,
  getVisualizerFriends: getVisualizerFriends,
  get_posts: getPosts,
};

module.exports = routes;


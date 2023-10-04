var AWS = require('aws-sdk'); 
AWS.config.update({region:'us-east-1'});
var db = new AWS.DynamoDB();
var stemmer = require('stemmer');
 
var check_login = function(username, callback) {
  // set paramters to serach for user with username
  username = username.toLowerCase();
  var params = {
      KeyConditions: {
        "username": {
          ComparisonOperator: 'EQ',
          AttributeValueList: [ { S: username } ]
        },
      },
      TableName: "users",
      // AttributesToGet: [ 'password', 'fullname' ]
  };

  // return password to check if matches with user input
  db.query(params, function(err, data) {
    if (err || data.Items.length === 0) {
      callback(err, null);
    } else {
      callback(err, data.Items[0].password.S);
    }
  });
}

var search_news = async function(words, callback) {
  // separate all of the words to contain multiple words in one search
  var keywords = words.trim().split(' ');
  // one for index of talks one for talks themselves
  var promises = [];
  var resultPromises = [];
	

  keywords.forEach((keyword) => {
	//prepare word to match inverse table entries
	  keyword = keyword.trim();
	  keyword = keyword.toLowerCase();
	  keyword = stemmer(keyword);
    var params = {
      KeyConditions: {
        "keyword": {
          ComparisonOperator: 'EQ',
          AttributeValueList: [ { S: keyword } ]
        },
      },
      TableName: "keyword_to_article",
    };
	  //add all promises to list
	  promises.push(db.query(params).promise());
  });
  
  // await promise in order to process sequentially
  const first = await Promise.all(promises).then(
	data => {
		var articles = [];
		// keep track of occurrences of each article
		var freq = new Map();
    console.log(data.length);
		data.forEach(keyword => {
      console.log(JSON.stringify(keyword));
      keyword.Items.forEach(article => {
        articles.push(article);
      })
			// console.log(dat);
      
		})
		console.log("here: ", articles);
		callback(null, articles);
	},
	err => {
		console.log(err);
  });
}

var get_user = function(username, callback) {
  username = username.toLowerCase();
  // parameters to check if user already exists with username
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
    if (err) {
      callback(err, "Error: database.js: getUser");
    } else if (data.Count == 0 || data.Items.length === 0) {
        callback(err, null);
    } else {
      callback(err, data);
    }
  });
}

var get_newsfeed = function(username, callback) {
  var params = {
    TableName: "news",
  }
  // search news table to get all news
  db.scan(params, function(err, data) {
    if (err || data.Items.length == 0) {
      console.log("No items");
      callback(err, null);
    } else {
      callback(err, data.Items);
    }
  });


};

var create_account = function(userFields, callback) {
  // check if user with same username already exists first
  var interests = [];
  userFields.interests.forEach(interest => {
    interests.push({"S": interest });
  });
  var first = userFields.firstName.toLowerCase();
  first = first.charAt(0).toUpperCase() + first.slice(1);
  var last = userFields.lastName.toLowerCase();
  last = last.charAt(0).toUpperCase() + last.slice(1);
  // if no such user, create user with required fields
  var params = {
    Item: {
      "username": {
        S: userFields.username.toLowerCase()
      },
      "password": { 
        S: userFields.password
      },
      "email": { 
        S: userFields.email
      },
      "firstName": { 
        S: first
      },
      "lastName": { 
        S: last
      },
      "affiliation": { 
        S: userFields.affiliation
      },
      "birthday": { 
        S: userFields.birthday
      },
      "interests": {
        L: interests
      },
      "friends": {
        // initially empty friends list for new account
        L: []
      },
      "posts": {
        // initially empty posts list for new account
        L: []
      },
      "online": {
        BOOL: true
      },
      // initially set room list to empty
      "rooms": { 
        L: [] 
      },
      "likedArticles": {
        L: []
      },
      "profilePicture" : {
        S: userFields.profilePicture
      }

    },
    TableName: "users",
    ReturnValues: 'NONE'
  };

  // add user to table
  db.putItem(params, function(err, data) {
    if (err)
      callback(err, null);
    else
      callback(err, 'Success');
  });
}

var create_post = function(username, type, toUser, postFields, callback) {
  var params = {
    Item: {
      "ID": {
        N: postFields.ID
      },
      "user" : {
        S: username
      },
      "content": { 
        S: postFields.content
      },
      "date": { 
        S: postFields.date
      },
      "commenters": { 
        L: []
      },
      "comments": { 
        L: []
      },
      "type": {
        S: type
      },
      "toUser": {
        S: toUser
      }
    },
    TableName: "posts",
    ReturnValues: 'NONE'
  };

  // add post to posts table
  db.putItem(params, function(err, data) {
    if (err) {
    console.log(err);
      callback(err, null);
    }
    else {
      callback(err, 'Success');
    }
  });
}


var add_comment = function(username, ID, field, comment, callback) {
  var commentToAdd = [{ "S": comment }];
  var userToAdd = [{"S": username}];
  var params = {
    TableName: "posts",
    Key: {
      ID: {
        N: ID
      } 
    },
    ExpressionAttributeValues: {
      ":val": {
        L: commentToAdd
      },
    },
    ExpressionAttributeNames: {
      "#fieldToUpdate": field,
    },
    UpdateExpression: "set #fieldToUpdate = list_append(#fieldToUpdate, :val)",
  };

  db.updateItem(params, function(err, data) {
    if (err) {
      console.log(err);
      callback(err, null);
    }
    else {
      var params = {
        TableName: "posts",
        Key: {
          ID: {
            N: ID
          } 
        },
        ExpressionAttributeValues: {
          ":val": {
            L: userToAdd
          }
        },
        ExpressionAttributeNames: {
          "#fieldToUpdate": "commenters",
        },
        UpdateExpression: "set #fieldToUpdate = list_append(#fieldToUpdate, :val)",
      };
      
      db.updateItem(params, function(err, data) {
        if (err) {
          console.log(err);
          callback(err, null);
        }
        else {
          callback(err, data);
        }
      });

    }
      
  });
}

var update_online = function(username, field, fieldValue, callback) {
  var params = {
    TableName: "users",
    Key: {
      username: {
        S: username
      } 
    },
    UpdateExpression: "set #fieldToUpdate = :val",
    ExpressionAttributeNames: {
      "#fieldToUpdate": field,
    },
    ExpressionAttributeValues: {
      ":val": {
        BOOL: fieldValue
      }
    },
  };

  db.updateItem(params, function(err, data) {
    if (err) {
      console.log(err);
      callback(err, null);
    } else {
      callback(err, data);
    }
  });
}

var update_last_active = function(username, callback) {
  var params = {
    TableName: "users",
    Key: {
      username: {
        S: username
      } 
    },
    ExpressionAttributeValues: {
      ":val": {
        N: Date.now()
      },
    },
    ExpressionAttributeNames: {
      "#fieldToUpdate": lastactivetime,
    },
    UpdateExpression: "set #fieldToUpdate = :val, #fieldToUpdate2 = :val2",
  };

  db.updateItem(params, function(err, data) {
    if (err) {
      console.log(err);
      callback(err, null);
    }
    else
      callback(err, data);
  });

}

var remove_interest_to_user = function(username, interests, callback) {
  let promises = new Array();
	for (let i = 0; i < interests.length; i++) {
		let interest = interests[i].toUpperCase();
		let params = {
      Key: {
        "interest": {
          S: interest
         }, 
        "username": {
          S: username
         }
       }, 
      TableName: "interest_to_user",
      ReturnValues: 'NONE'
		};
		promises.push(db.deleteItem(params).promise())		
	}
	
	return Promise.all(promises).then(
		success => {callback(null, success);},
		err => {callback(err, null);}
	);
}

//initalize interest to user table entries/mapping
var add_interest_to_user = function(username, interests, callback) {
  let promises = new Array();
	for (let i = 0; i < interests.length; i++) {
		let interest = interests[i].toUpperCase();
		let params = {
      Item: {
        "interest": {
          S: interest
        },
        "username" : {
          S: username
        },
      },
      TableName: "interest_to_user",
      ReturnValues: 'NONE'
		};
		promises.push(db.putItem(params).promise())		
	}
	
	return Promise.all(promises).then(
		success => {callback(null, success);},
		err => {callback(err, null);}
	);
}


var update_interest = function(username, field, interestsToAdd, callback) {
  var interestsLi = [];
  interestsToAdd.forEach(interest => {
    interestsLi.push({"S": interest});
  });
  var params = {
    TableName: "users",
    Key: {
      username: {
        S: username
      } 
    },
    ExpressionAttributeValues: {
      ":val": {
        L: interestsLi
      }
    },
    ExpressionAttributeNames: {
      "#fieldToUpdate": field,
    },
    UpdateExpression: "set #fieldToUpdate = :val",
  };

  db.updateItem(params, function(err, data) {
    if (err) {
      console.log(err);
      callback(err, null);
    }
    else
      callback(err, data);
  });
}

var add_post = function(username, field, postID, callback) {
  var postToAdd = [{ "N": postID }];
  var params = {
    TableName: "users",
    Key: {
      username: {
        S: username
      } 
    },
    ExpressionAttributeValues: {
      ":val": {
        L: postToAdd
      },
      ":empty": {
        L: []
      }
    },
    ExpressionAttributeNames: {
      "#fieldToUpdate": field,
    },
    UpdateExpression: "set #fieldToUpdate = list_append(if_not_exists(#fieldToUpdate, :empty), :val)",
  };

  db.updateItem(params, function(err, data) {
    if (err) {
      console.log(err);
      callback(err, null);
    }
    else
      callback(err, data);
  });
}

var add_friend = function(username, field, userToAdd, callback) {
  // store friends as list of usernames
  var friendsToAdd = [{"S": userToAdd}];
  var params = {
    TableName: "users",
    Key: {
      username: { S: username } 
    },
    ExpressionAttributeValues: {
      ":val": {
        L: friendsToAdd
      },
      ":empty": {
        L: []
      }
    },
    ExpressionAttributeNames: {
      "#fieldToUpdate": field,
    },
    UpdateExpression: "set #fieldToUpdate = list_append(if_not_exists(#fieldToUpdate, :empty), :val)",
  };

  db.updateItem(params, function(err, data) {
    if (err) {
      console.log(err);
      callback(err, null);
    }
    else
      callback(err, data);
  });
}

var add_article = function(username, link, callback) {
  // store friends as list of usernames
  var articleLink = [{"S": link}];
  var params = {
    TableName: "users",
    Key: {
      username: { S: username } 
    },
    ExpressionAttributeValues: {
      ":val": {
        L: articleLink
      },
      ":empty": {
        L: []
      }
    },
    ExpressionAttributeNames: {
      "#fieldToUpdate": "likedArticles",
    },
    UpdateExpression: "set #fieldToUpdate = list_append(if_not_exists(#fieldToUpdate, :empty), :val)",
  };

  db.updateItem(params, function(err, data) {
    if (err) {
      console.log(err);
      callback(err, null);
    }
    else
      callback(err, data);
  });
}

var create_article_to_user = function(username, link, callback) {
  var params = {
    Item: {
      "link": {
        S: link
      },
      "username" : {
        S: username
      }
    },
    TableName: "article_to_user",
    ReturnValues: 'NONE'
  };

  // add post to posts table
  db.putItem(params, function(err, data) {
    if (err) {
    console.log(err);
      callback(err, null);
    }
    else {
      callback(err, 'Success');
    }
  });
}

// call this in front-end only with friend name

var delete_friend = function(username, index, callback) {
  // store friends as list of usernames
	
  var params = {
    TableName: "users",
    Key: {
      username: {
        S: username
      } 
    },
    UpdateExpression: "REMOVE friends[" + index + "]",
  };

  db.updateItem(params, function(err, data) {
    if (err) {
      console.log(err);
      callback(err, null);
    }
    else
      callback(err, data);
  });
}

var get_users = function(callback) {
  var params = {
    TableName: "users",
  }

  // search users table to get all users
  db.scan(params, function(err, data) {
    if (err || data.Items.length == 0) {
      callback(err, null);
    } else {
      callback(err, data.Items);
    }
  });
}

var get_posts = function(callback) {
  var params = {
    TableName: "posts",
  }

  // search users table to get all users
  db.scan(params, function(err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(err, data.Items);
    }
  });
}

var search_name = function(input, callback) {
	var searchedList = input.split(" ");
	searchedList[0] = searchedList[0].toLowerCase();
	var first = searchedList[0].charAt(0).toUpperCase() + searchedList[0].slice(1);
	var last = "";
	if (searchedList.length > 1) {
		searchedList[1] = searchedList[1].toLowerCase();
		last = searchedList[1].charAt(0).toUpperCase() + searchedList[1].slice(1);
	}
	var params = {
		TableName: "users",
		FilterExpression: "contains(firstName, :first) AND contains(lastName, :last)",
		ExpressionAttributeValues: {
			":first" : {
				S: first
			},
			":last" : {
				S: last
			}
		}
			
	}
	db.scan(params, function(err, data) {
		
		callback(err, data);
	});
}

var get_friends = function(username, callback){
  var params = {
    TableName: "users",
    Key: {username: {S: username}},
    ExpressionAttributeValues: {":val": { L: friends}}
  };
  db.query(params, function(err, data) {
    if (err) {
      console.log(err);
    } else {
      console.log(data);
    }
  })
}

var get_friends_info = function(friends, callback) {	
	let promises = new Array();
	for (let i = 0; i < friends.length; i++) {
		let friend = friends[i].S;
		let params = {
	 	  TableName : "users",
			KeyConditionExpression: "#un = :string1",
			ExpressionAttributeNames: {"#un": "username"},
			ExpressionAttributeValues: {":string1": {"S" : friend }}
		};
		promises.push(db.query(params).promise())		
	}
	
	return Promise.all(promises).then(
		success => {
			let friends = [];
			for (let i = 0; i < success.length; i++) {
				friends.push(success[i].Items);
			}
			return friends;
		},
		error => {console.log(error)}
	);
}

var get_article_by_url = function(url, callback) {
 
  var params = {
    KeyConditions: {
      "link": {
        ComparisonOperator: 'EQ',
        AttributeValueList: [ { S: url } ]
      },
    },
    TableName: "news",
  };
 
  // return all matching users
  db.query(params, function(err, data) {
    if (err) {
      console.log(err);
    } else {
      console.log("by url: ", data);
      callback(err, data);
    }
  })
};
 
var get_recommended_articles = function(username, callback) {
 
  var params = {
    KeyConditions: {
      "username": {
        ComparisonOperator: 'EQ',
        AttributeValueList: [ { S: username.toLowerCase() } ]
      },
    },
    TableName: "adsorption",
  };
 
  // return all matching users
  db.query(params, function(err, data) {
    if (err) {
      console.log(err);
    } else {
      console.log("adsoprtion: ", data);
      callback(err, data);
    }
  })
};

var database = { 
  checklogin: check_login,
  createaccount: create_account,
  createPost: create_post,
  getUser: get_user,
  getPosts: get_posts,
  updateOnline: update_online,
  getUsers: get_users,
  getNewsfeed: get_newsfeed,
  searchNews: search_news,
  addFriend: add_friend,
  addArticle: add_article,
  createArticleToUser: create_article_to_user,
  deleteFriend: delete_friend,
  updateInterest: update_interest,
  addPost: add_post,
  addComment: add_comment,
  searchName: search_name,
  getFriends: get_friends,
  getFriendsInfo: get_friends_info,
  updateLastActive: update_last_active,
  addInterestToUser: add_interest_to_user,
  removeInterestToUser: remove_interest_to_user,
  getRecommendedArticles: get_recommended_articles,
  getArticleByUrl: get_article_by_url,
};

module.exports = database;
                                        
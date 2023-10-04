var db = require('../models/database.js');
var CryptoJS = require("crypto-js");
var chatdb = require('../models/chat_model.js');
var userdb = require('../models/update_user_model.js');
var postDB = require('../models/post_database.js');
const { update_interest } = require('./routes.js');
const { update_active } = require('../models/update_user_model.js');


var toUpdateUser = function(req, res) {
    var username = req.session.username;
    console.log("Session's username " + username);
    if (username) {
        db.getUser(username, function(err, data){
            if(err){
                console.log("toUpdateUser error: " + err);
                res.render('update_user.ejs', {success: false, message: 'Error getting user', username: username,
                profilePicture: null, affiliation: null, interests: null, email: null});
            } else {
                var userObj = data.Items[0];
                var interests = userObj.interests.L;
                var affiliation = userObj.affiliation.S;
                var profilePicture = userObj.profilePicture.S;
                console.log("Affiliation is " + affiliation);
                var email = userObj.email.S;
                res.render('update_user.ejs', {message: null, username: username, 
                    email: email, affiliation: affiliation, interests: interests, 
                    profilePicture : profilePicture});
            }
        });
    } else {
        res.render('login.ejs', {message: "Log in first!"});
    }
}

var updateAffiliation = function(req, res) {
    console.log("updateAffiliation called");
    var username = req.session.username;
    var affiliation = req.body.affiliation;
    userdb.update_attribute(username, "affiliation", affiliation, function(err, data) {
        if (err) {
            console.log("Error updating affiliation: " + err);
        return res.send( {success: false, message: 'Error updating affiliation',
         username: username});
        } else {
        var ID = (new Date()).getTime().toString();
        var content = 'Affiliation updated to ' + affiliation;
        var date =(new Date()).toLocaleString();
        var postFields = {ID, content, date};
        db.createPost(username, "Status Update", username, postFields, function(err, data) {
            if (err) {
                console.log(err);
                return res.send({success: false, message: err, result: null});
            } else {        
                db.addPost(username, "posts", ID, function(err, data) {
                    if (err) {
                        console.log("ERROR ADDING IT TO USER POSTS");
                        return res.send({success: false, message: err, result: null});
                    } else {
                        console.log("Successfully status update post");
                        return res.send( {success: true, message: 'Affiliation updated to ' + affiliation + '!', username: username});
                    }
                });
            }
        });
        
        } 
    });
}

var updateEmail = function(req, res){
    var username = req.session.username;
    var email = req.body.email;
    userdb.update_attribute(username, "email", email, function(err, data) {
        if (err) {
            console.log("Error updating email: " + err);
        res.send({success: false, message: 'Error updating affiliation', username: username});
        } else {
            console.log("Email updated to " + email);
        res.send({success: true, 
            message: 'Email updated to ' + email + '!', username: username});
        } 
    });
}

var updatePassword = function(req,res){
    var username = req.session.username;
    var password = CryptoJS.SHA256(req.body.password).toString();

    userdb.update_attribute(username, "password", password, function(err, data) {
        if (err) {
            console.log("Error updating password: " + err);
            res.send({success: false, message: 'Error updating affiliation', username: username});
        } else {
            console.log("Password update success");
            res.send({success: true, message: 'Password updated!', username: username});
        } 
    });
}

var updateProfilePicture = function(req,res){
    var username = req.session.username;
    var newProfilePicture = req.body.profilePicture;
    userdb.update_attribute(username, "profilePicture", newProfilePicture, function(err, data) {
        if (err) {
            console.log("Error updating ProfilePicture: " + err);
            res.send({success: false, message: 'Error updating ProfilePicture', username: username});
        } else {
            console.log("ProfilePicture update success");
            res.send({success: true, message: 'ProfilePicture updated!', username: username});
        } 
    });
}

var updateActive = function(req,res){
    var username = req.session.username;

    userdb.update_attribute(username, "online", true, function(err, data) {
        if (err) {
            console.log("Error updating password: " + err);
            res.send({success: false, message: 'Error updating affiliation', username: username});
        } else {
            console.log("Password update success");
            res.send({success: true, message: 'Password updated!', username: username});
        } 
    });
}

var updateInactive = function(req,res){
    var username = req.session.username;

    userdb.update_attribute(username, "online", false, function(err, data) {
        if (err) {
            console.log("Error updating password: " + err);
            res.send({success: false, message: 'Error updating affiliation', username: username});
        } else {
            console.log("Password update success");
            res.send({success: true, message: 'Password updated!', username: username});
        } 
    });
}

var updateInterests = function(req, res) {
    var username = req.session.username, newInterests = req.body.interests;

    db.getUser(username, function(err, data) {
        if (err) {
            return res.send({success: false, message: err, result: null});
        } else {
            // if user is successfully retrieved
            var user = data.Items[0];
            // all initial interests
            var oldInterests = []
            user.interests.L.forEach(interest => oldInterests.push(interest.S));

            // shared interests
            var sharedInterests = new Set();
            oldInterests.forEach(interest => {
                if (newInterests.includes(interest)) sharedInterests.add(interest);
            })
            console.log("shared: ", sharedInterests);

            // set of only old interests (to remove)
            oldInterests = oldInterests.filter(interest => !newInterests.includes(interest));
            console.log("old: ", oldInterests);

            // set of only new interests (to add)
            var interestsToAdd = newInterests.filter(interest => !sharedInterests.has(interest));
            console.log("new: ", interestsToAdd);

            // update interests to those that are selected
            db.updateInterest(username, "interests", newInterests, function(err, data) {
                if (err) return res.send({success: false, message: err, result: null});
                else {
                    // if interests are successfully updated, need to add/remove to interest_to_user table accordingly
                    db.addInterestToUser(username, interestsToAdd, function(err,data) {
                        if (err) return res.send({success: false, message: err, result: null});
                        else {
                            db.removeInterestToUser(username, oldInterests, function(err,data) {
                                if (err) return res.send({success: false, message: err, result: null});
                                else {
                                    var ID = (new Date()).getTime().toString();
                                    var content = 'Interests updated to: ' + newInterests;
                                    var date =(new Date()).toLocaleString();
                                    var postFields = {ID, content, date};
                                    db.createPost(username, "Status Update", username, postFields, function(err, data) {
                                        if (err) {
                                            console.log(err);
                                            return res.send({success: false, message: err, result: null});
                                        } else {        
                                            db.addPost(username, "posts", ID, function(err, data) {
                                                if (err) {
                                                    return res.send({success: false, message: err, result: null});
                                                } else {
                                                    return res.send({success: true, result: data});
                                                }
                                            });
                                        }
                                    });
                                    
                                };
                            });
                        }
                      })
                }
            });
        }
    });
  }

var update_user_routes = { 
    to_update_user: toUpdateUser,
    update_affiliation: updateAffiliation,
    update_email: updateEmail,
    update_password: updatePassword,
    update_profile_picture: updateProfilePicture,
    update_interests: updateInterests,
    update_active: updateActive,
    update_inactive: updateInactive,
};

module.exports = update_user_routes;
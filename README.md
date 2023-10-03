# netsbook

## Overview
Netsbook is our group G20’s implementation of a “mini-Facebook” application for the NETS2120 final project. NetsBook incorporates most of the key features of Facebook including: a login/signup page, personalized news feed that shows users the most relevant and interesting content from their friends and pages they follow; the ability to share a variety of different types of content, including text, images, and links; a messaging system that allows users to communicate privately with their friends, family, and larger groups; a detailed and interactive friend visualizer; and a page for news and articles.

## Technical Description of Components
### General Implementation Details:
Uses socket.io to consistently update if a user is online or not
Three different types of posts: status updates, new friendships, and wall posts
All posts will be visible on the owner’s personal wall
A user’s friends can see any post that their other friends have posted on the users’ walls
Users have affiliations which allow users to see people associated with the same affiliation whom are not currently friends with them through the friend visualizer
All posts are sorted in chronological order
Users are able to update profile and change affiliation, email, password, profile picture (extra feature)
Passwords are hashed using SHA256
### Home Page:
Displays table of logged-in user’s friends and their respective data
Displays posts that belong to you and your friends
Displays status updates that belong to you and your friends 
Displays new friendships that were made between you and your friends
Comments can be added and seen immediately
Includes search and autofill feature to look up potential friends
Includes infinite scroller to see posts on wall (extra feature)
### User Pages:
Can add friend, post status updates, update user attributes by making DynamoDB requests through ajax and non-ajax calls
When affiliation and interests change, a status update is put into DynamoDB table and shows in front end
### Chat Functionality:
Each user in users table has a room_id attribute (L), each room in rooms table has a list of msg_ids, each msg in messages table contains data about text, sender, etc.
In chat_room.ejs, instantaneous messaging is implemented with socket.io message emits and DynamoDB updates to messages and rooms tables.
For any friend not in the current chat room, you can
Invite to current room (extra feature)
Create new room with new user added to existing users
### News Recommendation:
Loaded data in adsorption tables using DynamoDB’s Java SDK, with partition key of “username” and sort key of “url” in order to aggregate unique (partition, sort) pairings
Tables that we pull from to populate RDDs are users and article_to_category
Spark computation done in SocialRankJob.java file that computes adsorption by pulling data from Dynamo and running spark and then putting back into dynamo
In the front end, we can query this adsorption table for updated weight values and algorithm results
Based on this, query news table for the corresponding news objects
Other:
When a user logs in and has a tab open to any of our pages, the user is marked as online in the DynamoDB table. Solved using socket.
When you close the tab, socket automatically emits a disconnect message and updates the DynamoDB table to show that the user is offline.

## Design Decisions
The biggest decision was the way we designed our table. Because we didn’t modularize each part of a user, for example, it could be difficult for scalability. However, it is extremely efficient to return one singular user object with a variety of fields, rather than separating these fields into separate tables
Used HTML, CSS, EJS, Bootstrap for frontend
Used DynamoDB for database, tables were: users, rooms, posts, news, messages, adsorption, keyword_to_article, interest_to_user, interest_to_article, article_to_user
Used Express for server
When running the adsorption algorithm, we had to decide between using livy vs. DynamoDB for storing the computations. Livy was a more comfortable and efficient choice. 

## Lessons Learned
Create tables first and use that to design overall system
Understanding that for each user, we need to store username, affiliation, first name, last name, birthday, email, friends, interests, liked articles, online status, password
Leads to better managing of DynamoDB tables. Unnecessary to continuously delete data items because of structural changes
Organization is key
If there are certain route functions for the news page, for example, we should keep that in a separate file for improved readability
Map out file structure first instead of on the fly. Especially in group projects, keeps everyone on the same page
Create one underlying front end theme with CSS features included so unnecessary to keep changing all files every time small change is made
Assign tasks based on strengths
Two people who are most experienced with Livy should coordinate times to work together
Debugging and testing should be done always after completion of a feature

## Extra Credit Features
Profile Pictures for all users
Stored as urls in the users DynamoDB table
Loaded in front end with HTML
Users begin with default Penn Logo profile picture if optional signup field not filled
Profile pictures updated instantly when changed
Displayed in the home page, chat rooms page, individual chat rooms, and more
Invite user to current chat room (instead of creating a new room with the new user)
Makes an update request to DynamoDB rooms and users tables rather than put
Infinite scrolling in home page
Created onScroll listener to detect when user scrolls
Threshold variable to determine if ready to loadMoreContent()
Loader-circle that appears when loading and disappears when loading has completed

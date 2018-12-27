// Use msan697 database. 
print("\nQ1");
//Your code
use msan697
// Use msan697 database. 
print("\nQ2");
//Your code
db.business.find({}).count()
// Find a document of "Chinese" restaurant called "May May Kitchen" and print the document without _id.
print("\nQ3");
//Your code
db.business.find({"cuisine":"Chinese", "name":"May May Kitchen" }, {"_id":0}).pretty()
// Count the number of restaurants with either “Mexican” or “Korean” cuisine type. 
print("\nQ4");
//Your code
db.business.find({"cuisine":{$in:["Mexican", "Korean"]}}).count()
// Count the number of restaurants which name include the world "American" (Case-sensitive). 
print("\nQ5");
//Your code
db.business.find({"name":{$regex:"American"}}).count()
// Find "Indian" restaurants and list only their names in ascending order. 
print("\nQ6");
//Your code
db.business.aggregate({$match:{"cuisine":"Indian"}},{$project:{"_id":0, "name":1}}, {$sort:{"name":1}})
// Print only the grades of a restaurant called "Kismat Indian Cuisine" sorted by date in ascending order. 
print("\nQ7");
//Your code
db.business.aggregate({$match:{"name":"Kismat Indian Cuisine"}},{$project:{"_id":0, "grades":1}}, {$unwind:"$grades"}, {$sort:{"grades.date":1}})
// Print only the average grades of a restaurant called "Kismat Indian Cuisine". 
print("\nQ8");
//Your code
db.business.aggregate({$match:{"name":"Kismat Indian Cuisine"}},{$project:{"grades.score":1}}, {$unwind:"$grades"}, {$group:{"_id":"$_id", "average_score":{$avg:"$grades.score"}}}, {$project:{"_id":0}})
// Print a business which address is "building" : "265-15", "coord" : [ -73.7032601, 40.7386417 ], "street" : "Hillside Avenue", "zipcode" : "11004" without _id. 
print("\nQ9");
//Your code
db.business.findOne({"address":{"building":"265-15","coord":[-73.7032601, 40.7386417], "street":"Hillside Avenue", "zipcode":"11004"}}, {"_id":0})
// Drop the business collection.
print("\nQ10");
//Your code
db.business.drop()
print("\n");



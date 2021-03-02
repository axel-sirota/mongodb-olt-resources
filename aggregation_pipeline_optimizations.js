// Start the MongoDB Container 

docker run --name mongo -d mongo
docker exec -i mongo sh -c 'mongoimport -d oreilly -c students --drop --type json' < students.json

// Check that Mongodb adds a projection on non-used fields

db.students.explain("executionStats").aggregate( [
    { $group: { _id: "$grade", frequency: { $sum: 1 } } },
    { $match: { "frequency": { $gt : 10 } } }
 ] )

// Let's see what happen if we filter by an initial field

db.students.explain("executionStats").aggregate( [
    { $group: { _id: "$grade", frequency: { $sum: 1 } } },
    { $match: { "frequency": { $gt : 10 } } },
    { $match: { "age" : { $lt : 15 }}  }
 ] )

// It tried to merge the match statements, but if we add an index?

db.students.createIndex({ "age" : 1})
db.students.createIndex({ "grade" : 1})
db.students.createIndex({ "age":1, "grade" : 1})
db.students.createIndex({ "grade":1, "age" : 1})

db.students.explain("executionStats").aggregate( [
    { $match: { "age" : { $lt : 15 }}  },
    { $group: { _id: "$grade", frequency: { $sum: 1 } } },
    { $match: { "frequency": { $gt : 10 } } }
 ])

// It doesn't seem to help.

// If we want we can force an index

 db.students.explain("executionStats").aggregate( [
    { $match: { "age" : { $lt : 15 }}  },
    { $group: { _id: "$grade", frequency: { $sum: 1 } } },
    { $match: { "frequency": { $gt : 10 } } }
 ] , { hint: "age_1"} )

// If we add a sort early, it can use indexes too! And it merges the operation!

db.students.explain("executionStats").aggregate( [
    { $sort : {  "age": -1 }},
    { $match: { "age" : { $lt : 15 }}  },
    { $group: { _id: "$grade", frequency: { $sum: 1 } } },
    { $match: { "frequency": { $gt : 10 } } }
 ] )

// sort + limit merge ; match merge

db.students.explain("executionStats").aggregate( [
    { $sort : {  "age": -1 } },
    { $limit: 100000 },
    { $match: { "age" : { $lt : 15 }}  },
    { $group: { _id: "$grade", frequency: { $sum: 1 } } },
    { $match: { "frequency": { $gt : 10 } } }
 ] )

 // If we didn't use the index, it would be slow because of the in memory sort!

 db.students.explain("executionStats").aggregate( [
    { $sort : {  "age": -1 } },
    { $limit: 100000 },
    { $match: { "age" : { $lt : 15 }}  },
    { $group: { _id: "$grade", frequency: { $sum: 1 } } },
    { $match: { "frequency": { $gt : 10 } } }
 ] , { hint: "_id_" })

// In case of doubt, we can turn on the profiler!

db.setProfilingLevel(2, { slowms: 200 })


db.students.aggregate( [
    { $sort : {  "age": -1 }},
    { $match: { "age" : { $lt : 15 }}  },
    { $group: { _id: "$grade", frequency: { $sum: 1 } } },
    { $match: { "frequency": { $gt : 10 } } }
 ] )

// And we find out operation

db.system.profile.find( { millis : { $gt : 1000 } } ).pretty()
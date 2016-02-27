/**
 * Documentation of C# driver 1.10:
 * http://mongodb.github.io/mongo-csharp-driver/1.11/getting_started/
 * c# Driver for MongoDBprovided by http://answers.unity3d.com/questions/618708/unity-and-mongodb-saas.html
 */ 


using UnityEngine;
using System; //
using System.Collections;
using System.Collections.Generic;  // Lists

using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Builders;  
using MongoDB.Driver.GridFS;  
using MongoDB.Driver.Linq;  

public class HelloMongo : MonoBehaviour {

	string connectionString = "mongodb://localhost:27017";
	// string connectionString = "mongodb://gravityhunter.mi.hdm-stuttgart.de:27017";

	void Start () {


		/*
		 * 1. establish connection
		 */



		var client = new MongoClient(connectionString);
		var server = client.GetServer(); 
		var database = server.GetDatabase("test");
		var playercollection= database.GetCollection<BsonDocument>("players");
		Debug.Log ("1. ESTABLISHED CONNECTION");



		/*
		 * 2. INSERT new dataset: INSERT INTO players (email, name, scores, level) VALUES("..","..","..","..");
		 */



		playercollection.Insert(new BsonDocument{
			{ "level", 7 },
			{ "name", "Fabi" },
			{ "scores", 4711 },
			{ "email", "ff023@hdm-s.de" }
		});
		Debug.Log ("2. INSERTED A DOC");



		/*
		 * 3. INSERT multiple datasets
		 */



		BsonDocument [] batch={
			new BsonDocument{
				{ "level", 3 },
				{ "name", "Kaki" },
				{ "scores", 27017 },
				{ "email", new BsonDocument{
					{ "private", "k@k.i" },
					{ "business", new BsonArray{
						{"mr@ka.ki"},{"dr@ka.ki"}
					}}
				}}
			}, 
			new BsonDocument{
				{ "level", 5 },
				{ "name", "Gabi" },
				{ "scores", 6454 },
				{ "email", "g@b.i" }
			},
			new BsonDocument{
				{ "level", 5 },
				{ "name", "Vati" },
				{ "scores", 1936 },
				{ "email", "g@b.i" }
			} 
		};
		playercollection.InsertBatch (batch);
		Debug.Log ("3. INSERTED MULTIPLE DOCS");



		/*
		 * 4. SELECT * FROM players
		 */



		foreach (var document in playercollection.FindAll()) {
			Debug.Log ("4. SELECT ALL DOCS: \n" + document);
		}

		// logs
		// { "_id" : ObjectId("56d203d42a1e03167dc1ad29"), "level" : 7, "name" : "Fabi", "scores" : 4711, "email" : "ff023@hdm-s.de" }
		// { "_id" : ObjectId("56d203d42a1e03167dc1ad2a"), "level" : 3, "name" : "Kaki", "scores" : 27017, "email" : { "private" : "k@k.i", "business" : ["mr@ka.ki", "dr@ka.ki"] } }
		// { "_id" : ObjectId("56d203d42a1e03167dc1ad2b"), "level" : 5, "name" : "Gabi", "scores" : 6454, "email" : "g@b.i" }
		// { "_id" : ObjectId("56d203d42a1e03167dc1ad2c"), "level" : 5, "name" : "Vati", "scores" : 1936, "email" : "g@b.i" }



		/*
		 * 5. SELECT the first doc
		 */



		Debug.Log("5. SELECT FIRST DOC: \n" + playercollection.FindOne ().ToString());

		// logs { "_id" : ObjectId("56d203d42a1e03167dc1ad29"), "level" : 7, "name" : "Fabi", "scores" : 4711, "email" : "ff023@hdm-s.de" }



		/*
		 * 6. SELECT * FROM players WHERE name = 'Fabi'
		 */



		foreach (var document in playercollection.Find(new QueryDocument("name", "Kaki"))){
			Debug.Log ("6. SELECT DOC WHERE: \n" + document);
		}

		// logs { "_id" : ObjectId("56d203d42a1e03167dc1ad2a"), "level" : 3, "name" : "Kaki", "scores" : 27017, "email" : { "private" : "k@k.i", "business" : ["mr@ka.ki", "dr@ka.ki"] } }



		/*
		 * 7. macht dasselbe 
		 */



		var query7 = new QueryDocument("name", "Gabi");
		//var query7 = Query.EQ("name", "Gabi"); // same as line above
		foreach (var document in playercollection.Find(query7)) {
			Debug.Log ("7. SELECT DOC WHERE: \n" + document);
		}

		// logs { "_id" : ObjectId("56d203d42a1e03167dc1ad2b"), "level" : 5, "name" : "Gabi", "scores" : 6454, "email" : "g@b.i" }



		/*
		 * 8. SELECT scores FROM players WHERE name = 'Fabi'
		 */



		foreach (var document in playercollection.Find(new QueryDocument("name", "Kaki"))){
			Debug.Log ("8. SELECT DOC PART WHERE: \n" + document["scores"]);
		}

		// logs 27017


	
		/*
		 * 9. SELECT scores FROM players WHERE {[{nested}]}
		 */



		foreach (var document in playercollection.Find(new QueryDocument("name", "Kaki"))){
			Debug.Log ("9. SELECT NESTED DOC PART WHERE:  \n" + document["email"]["business"][0]);
		}
			
		// logs mr@ka.ki



		/*
		 * 10. SELECT * FROM players -> Limit 2 entries
		 */



		foreach (var document in playercollection.FindAll().SetLimit(2)) {
			Debug.Log ("10. SELECT DOC WHERE LIMIT: \n" + document);
		}

		// logs 
		// { "_id" : ObjectId("56d203d42a1e03167dc1ad29"), "level" : 7, "name" : "Fabi", "scores" : 4711, "email" : "ff023@hdm-s.de" }
		// { "_id" : ObjectId("56d203d42a1e03167dc1ad2a"), "level" : 3, "name" : "Kaki", "scores" : 27017, "email" : { "private" : "k@k.i", "business" : ["mr@ka.ki", "dr@ka.ki"] } }



		/*
		 * 11. DELETE FROM players WHERE name = "Vati"
		 */



		playercollection.Remove(Query.EQ("name", "Vati"));
		Debug.Log ("11. DELETE DOC WHERE (vati)");



		/*
		 * 12. SELECT * FROM players -> sort
		 */



		foreach (var document in playercollection.FindAll()
			.SetSortOrder(SortBy.Descending("scores"))) {
			Debug.Log ("12. SELECT DOC SORT: \n" + document);
		}

		// logs 
		// { "_id" : ObjectId("56d203d42a1e03167dc1ad2a"), "level" : 3, "name" : "Kaki", "scores" : 27017, "email" : { "private" : "k@k.i", "business" : ["mr@ka.ki", "dr@ka.ki"] } }
		// { "_id" : ObjectId("56d203d42a1e03167dc1ad2b"), "level" : 5, "name" : "Gabi", "scores" : 6454, "email" : "g@b.i" }
		// { "_id" : ObjectId("56d203d42a1e03167dc1ad29"), "level" : 7, "name" : "Fabi", "scores" : 4711, "email" : "ff023@hdm-s.de" }



		/*
		 * 13. SELECT * FROM players WHERE scores > 4712
		 */



		var query13 = Query.GT("scores", 4712); 
		foreach (var document in playercollection.Find(query13)) {
			Debug.Log ("13. SELECT DOC WHERE GREATER THAN: \n" + document);
		}

		// logs 
		// { "_id" : ObjectId("56d203d42a1e03167dc1ad2a"), "level" : 3, "name" : "Kaki", "scores" : 27017, "email" : { "private" : "k@k.i", "business" : ["mr@ka.ki", "dr@ka.ki"] } }
		// { "_id" : ObjectId("56d203d42a1e03167dc1ad2b"), "level" : 5, "name" : "Gabi", "scores" : 6454, "email" : "g@b.i" }



		/*
		 * 14. UPDATE players SET level = 11 WHERE name = "Fabi AND email = "ff023"
		 */



		var where14 = new QueryDocument{
			{"name", "Gabi"},
			{"email", "g@b.i"}
		};
		var set14 = new UpdateDocument {
			{ "$set", new BsonDocument ("level", 11) }
		};
		playercollection.Update (where14, set14);
		Debug.Log ("14. UPDATE DOC SET WHERE");



		/*
		 * 15. If value exists: UPDATE, if not: INSERT
		 */



		var whereClause15 = Query.And(
			Query.EQ("name", "Fabi")
		);
		BsonDocument query15 = playercollection.FindOne(whereClause15);
		if (query15 != null) {
			query15["level"] = 11;
			playercollection.Save(query15);
		}
		Debug.Log ("15. UPDATE DOC SET WHERE / IF NOT EXISTS INSERT");



		/*
		 * 16. COUNT docs
		 */



		Debug.Log("16. COUNT DOCS: \n" + playercollection.Count());

		// logs 3
	}
}
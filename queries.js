// queries.js - Script with MongoDB queries
// Run with: node queries.js

const { MongoClient } = require("mongodb");

// Connection URI (update if using Atlas)
const uri = "mongodb://localhost:27017";

// Database and collection names
const dbName = "plp_bookstore";
const collectionName = "books";

async function runQueries() {
  const client = new MongoClient(uri);

  try {
    await client.connect();
    console.log("Connected to MongoDB");

    const db = client.db(dbName);
    const collection = db.collection(collectionName);

    // ====================
    // Task 2: CRUD Queries
    // ====================

    console.log("\n--- Find all books in Fiction genre ---");
    console.log(await collection.find({ genre: "Fiction" }).toArray());

    console.log("\n--- Find books published after 1950 ---");
    console.log(await collection.find({ published_year: { $gt: 1950 } }).toArray());

    console.log("\n--- Find books by George Orwell ---");
    console.log(await collection.find({ author: "George Orwell" }).toArray());

    console.log("\n--- Update price of '1984' ---");
    await collection.updateOne({ title: "1984" }, { $set: { price: 12.5 } });
    console.log("Updated price of 1984");

    console.log("\n--- Delete 'Moby Dick' ---");
    await collection.deleteOne({ title: "Moby Dick" });
    console.log("Deleted Moby Dick");

    // ========================
    // Task 3: Advanced Queries
    // ========================

    console.log("\n--- In-stock books published after 2000 ---");
    console.log(await collection.find({ in_stock: true, published_year: { $gt: 2000 } }).toArray());

    console.log("\n--- Projection (title, author, price only) ---");
    console.log(await collection.find({}, { projection: { title: 1, author: 1, price: 1, _id: 0 } }).toArray());

    console.log("\n--- Books sorted by price (ascending) ---");
    console.log(await collection.find().sort({ price: 1 }).toArray());

    console.log("\n--- Books sorted by price (descending) ---");
    console.log(await collection.find().sort({ price: -1 }).toArray());

    console.log("\n--- Pagination: Page 1 (5 books) ---");
    console.log(await collection.find().limit(5).skip(0).toArray());

    console.log("\n--- Pagination: Page 2 (next 5 books) ---");
    console.log(await collection.find().limit(5).skip(5).toArray());

    // =============================
    // Task 4: Aggregation Pipelines
    // =============================

    console.log("\n--- Average price of books by genre ---");
    console.log(await collection.aggregate([
      { $group: { _id: "$genre", avgPrice: { $avg: "$price" } } }
    ]).toArray());

    console.log("\n--- Author with the most books ---");
    console.log(await collection.aggregate([
      { $group: { _id: "$author", bookCount: { $sum: 1 } } },
      { $sort: { bookCount: -1 } },
      { $limit: 1 }
    ]).toArray());

    console.log("\n--- Books grouped by decade ---");
    console.log(await collection.aggregate([
      {
        $addFields: {
          decade: { $multiply: [ { $floor: { $divide: ["$published_year", 10] } }, 10 ] }
        }
      },
      { $group: { _id: "$decade", count: { $sum: 1 } } },
      { $sort: { _id: 1 } }
    ]).toArray());

    // ====================
    // Task 5: Indexing
    // ====================

    console.log("\n--- Creating indexes ---");
    await collection.createIndex({ title: 1 });
    await collection.createIndex({ author: 1, published_year: -1 });
    console.log("Indexes created on title and (author, published_year)");

    console.log("\n--- Performance Analysis with explain() ---");
    const explainResult = await collection.find({ title: "1984" }).explain("executionStats");
    console.log(JSON.stringify(explainResult.queryPlanner.winningPlan, null, 2));

  } catch (err) {
    console.error("Error:", err);
  } finally {
    await client.close();
    console.log("Connection closed");
  }
}

// Run the queries
runQueries().catch(console.error);

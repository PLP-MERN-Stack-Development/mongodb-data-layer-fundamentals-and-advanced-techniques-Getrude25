const { MongoClient } = require("mongodb");

// Local MongoDB
const uri = "mongodb://127.0.0.1:27017";

// MongoDB Atlas (if using Atlas, replace with your connection string)
// const uri = "mongodb+srv://Getruda:Test12345@plp-bookstore.lmyh4z6.mongodb.net/books

async function run() {
  const client = new MongoClient(uri);
  try {
    await client.connect();
    console.log("✅ Connected to MongoDB");
  } catch (err) {
    console.error("❌ Connection failed:", err);
  } finally {
    await client.close();
  }
}

run();

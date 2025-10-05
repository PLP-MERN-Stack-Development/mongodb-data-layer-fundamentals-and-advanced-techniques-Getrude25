const { MongoClient } = require('mongodb');

class BookstoreQueries {
    constructor() {
        this.uri = "mongodb://127.0.0.1:27017";
        this.client = new MongoClient(this.uri);
        this.database = null;
        this.books = null;
    }

    async connect() {
        try {
            await this.client.connect();
            console.log("✅ Connected to MongoDB");
            this.database = this.client.db("plp_bookstore");
            this.books = this.database.collection("books");
        } catch (error) {
            console.error("❌ Connection error:", error);
            throw error;
        }
    }

    async disconnect() {
        await this.client.close();
        console.log("🔌 Disconnected from MongoDB");
    }

    // TASK 2: Basic CRUD Operations
    async findAllBooksInGenre(genre) {
        return await this.books.find({ genre: genre }).toArray();
    }

    async findBooksPublishedAfter(year) {
        return await this.books.find({ published_year: { $gt: year } }).toArray();
    }

    async findBooksByAuthor(author) {
        return await this.books.find({ author: author }).toArray();
    }

    async updateBookPrice(title, newPrice) {
        const result = await this.books.updateOne(
            { title: title },
            { $set: { price: newPrice } }
        );
        return result;
    }

    async deleteBookByTitle(title) {
        const result = await this.books.deleteOne({ title: title });
        return result;
    }

    // TASK 3: Advanced Queries
    async findInStockBooksAfter2010() {
        return await this.books.find({
            in_stock: true,
            published_year: { $gt: 2010 }
        }).toArray();
    }

    async findBooksWithProjection(genre) {
        return await this.books.find(
            { genre: genre },
            { projection: { title: 1, author: 1, price: 1, _id: 0 } }
        ).toArray();
    }

    async sortBooksByPriceAscending() {
        return await this.books.find().sort({ price: 1 }).toArray();
    }

    async sortBooksByPriceDescending() {
        return await this.books.find().sort({ price: -1 }).toArray();
    }

    async getBooksPaginated(pageNumber, pageSize = 5) {
        const skip = (pageNumber - 1) * pageSize;
        return await this.books.find()
            .skip(skip)
            .limit(pageSize)
            .toArray();
    }

    // TASK 4: Aggregation Pipeline
    async averagePriceByGenre() {
        const pipeline = [
            {
                $group: {
                    _id: "$genre",
                    averagePrice: { $avg: "$price" },
                    totalBooks: { $sum: 1 }
                }
            },
            {
                $sort: { averagePrice: -1 }
            }
        ];
        return await this.books.aggregate(pipeline).toArray();
    }

    async authorWithMostBooks() {
        const pipeline = [
            {
                $group: {
                    _id: "$author",
                    bookCount: { $sum: 1 }
                }
            },
            {
                $sort: { bookCount: -1 }
            },
            {
                $limit: 1
            }
        ];
        return await this.books.aggregate(pipeline).toArray();
    }

    async booksByPublicationDecade() {
        const pipeline = [
            {
                $project: {
                    title: 1,
                    published_year: 1,
                    decade: {
                        $subtract: [
                            "$published_year",
                            { $mod: ["$published_year", 10] }
                        ]
                    }
                }
            },
            {
                $group: {
                    _id: "$decade",
                    bookCount: { $sum: 1 },
                    books: { $push: "$title" }
                }
            },
            {
                $sort: { _id: 1 }
            }
        ];
        return await this.books.aggregate(pipeline).toArray();
    }

    // TASK 5: Indexing
    async createIndexes() {
        console.log("📊 Creating indexes...");
        
        // Single index on title
        await this.books.createIndex({ title: 1 });
        console.log("✅ Created index on title field");
        
        // Compound index on author and published_year
        await this.books.createIndex({ author: 1, published_year: -1 });
        console.log("✅ Created compound index on author and published_year");
        
        // Verify indexes
        const indexes = await this.books.indexes();
        console.log("📋 Current indexes:", indexes.map(idx => idx.name));
    }

    async demonstrateIndexPerformance() {
        console.log("\n⚡ Performance Demonstration");
        
        // Without index (on a non-indexed field for comparison)
        console.log("Query without index (genre):");
        const explainGenre = await this.books.find({ genre: "Fiction" }).explain();
        console.log(`Documents examined: ${explainGenre.executionStats.totalDocsExamined}`);
        
        // With index (title)
        console.log("\nQuery with index (title):");
        const explainTitle = await this.books.find({ title: "The Great Gatsby" }).explain();
        console.log(`Documents examined: ${explainTitle.executionStats.totalDocsExamined}`);
    }
}

// Main execution function
async function main() {
    const bookstore = new BookstoreQueries();
    
    try {
        await bookstore.connect();
        
        console.log("🎯 MONGODB WEEK 1 ASSIGNMENT - PLP BOOKSTORE\n");
        
        // TASK 2: Basic CRUD Operations
        console.log("1. All Fiction books:");
        console.log(await bookstore.findAllBooksInGenre("Fiction"));
        
        console.log("\n2. Books published after 2000:");
        console.log(await bookstore.findBooksPublishedAfter(2000));
        
        console.log("\n3. Books by J.K. Rowling:");
        console.log(await bookstore.findBooksByAuthor("J.K. Rowling"));
        
        console.log("\n4. Updating price of 'The Great Gatsby' to 13.99:");
        console.log(await bookstore.updateBookPrice("The Great Gatsby", 13.99));
        
        console.log("\n5. Deleting '1984':");
        console.log(await bookstore.deleteBookByTitle("1984"));
        
        // TASK 3: Advanced Queries
        console.log("\n6. In-stock books published after 2010:");
        console.log(await bookstore.findInStockBooksAfter2010());
        
        console.log("\n7. Fantasy books with projection:");
        console.log(await bookstore.findBooksWithProjection("Fantasy"));
        
        console.log("\n8. Books sorted by price (ascending):");
        console.log(await bookstore.sortBooksByPriceAscending());
        
        console.log("\n9. Books sorted by price (descending):");
        console.log(await bookstore.sortBooksByPriceDescending());
        
        console.log("\n10. Pagination - Page 1:");
        console.log(await bookstore.getBooksPaginated(1));
        
        // TASK 4: Aggregation Pipeline
        console.log("\n11. Average price by genre:");
        console.log(await bookstore.averagePriceByGenre());
        
        console.log("\n12. Author with most books:");
        console.log(await bookstore.authorWithMostBooks());
        
        console.log("\n13. Books by publication decade:");
        console.log(await bookstore.booksByPublicationDecade());
        
        // TASK 5: Indexing
        console.log("\n14. Creating indexes:");
        await bookstore.createIndexes();
        
        console.log("\n15. Performance demonstration:");
        await bookstore.demonstrateIndexPerformance();
        
    } catch (error) {
        console.error("❌ Error in main execution:", error);
    } finally {
        await bookstore.disconnect();
    }
}

// Run if this file is executed directly
if (require.main === module) {
    main().catch(console.error);
}

module.exports = BookstoreQueries;
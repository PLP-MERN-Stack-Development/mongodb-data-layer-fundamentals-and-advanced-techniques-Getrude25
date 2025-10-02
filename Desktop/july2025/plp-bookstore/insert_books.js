const { MongoClient } = require('mongodb');

async function insertBooks() {
    const uri = "mongodb://localhost:27017";
    const client = new MongoClient(uri);
    
    try {
        await client.connect();
        console.log("Connected to MongoDB");
        
        const database = client.db("plp_bookstore");
        const books = database.collection("books");
        
        // Clear existing collection
        await books.deleteMany({});
        
        const bookData = [
            {
                title: "The Great Gatsby",
                author: "F. Scott Fitzgerald",
                genre: "Fiction",
                published_year: 1925,
                price: 12.99,
                in_stock: true,
                pages: 218,
                publisher: "Scribner"
            },
            {
                title: "To Kill a Mockingbird",
                author: "Harper Lee",
                genre: "Fiction",
                published_year: 1960,
                price: 14.99,
                in_stock: true,
                pages: 281,
                publisher: "J.B. Lippincott & Co."
            },
            {
                title: "1984",
                author: "George Orwell",
                genre: "Dystopian",
                published_year: 1949,
                price: 10.99,
                in_stock: false,
                pages: 328,
                publisher: "Secker & Warburg"
            },
            {
                title: "The Hobbit",
                author: "J.R.R. Tolkien",
                genre: "Fantasy",
                published_year: 1937,
                price: 16.99,
                in_stock: true,
                pages: 310,
                publisher: "George Allen & Unwin"
            },
            {
                title: "Pride and Prejudice",
                author: "Jane Austen",
                genre: "Romance",
                published_year: 1813,
                price: 9.99,
                in_stock: true,
                pages: 432,
                publisher: "T. Egerton"
            },
            {
                title: "The Catcher in the Rye",
                author: "J.D. Salinger",
                genre: "Fiction",
                published_year: 1951,
                price: 11.99,
                in_stock: false,
                pages: 234,
                publisher: "Little, Brown and Company"
            },
            {
                title: "Harry Potter and the Philosopher's Stone",
                author: "J.K. Rowling",
                genre: "Fantasy",
                published_year: 1997,
                price: 19.99,
                in_stock: true,
                pages: 223,
                publisher: "Bloomsbury"
            },
            {
                title: "The Da Vinci Code",
                author: "Dan Brown",
                genre: "Mystery",
                published_year: 2003,
                price: 15.99,
                in_stock: true,
                pages: 489,
                publisher: "Doubleday"
            },
            {
                title: "The Alchemist",
                author: "Paulo Coelho",
                genre: "Fiction",
                published_year: 1988,
                price: 13.99,
                in_stock: true,
                pages: 208,
                publisher: "HarperTorch"
            },
            {
                title: "The Hunger Games",
                author: "Suzanne Collins",
                genre: "Dystopian",
                published_year: 2008,
                price: 12.99,
                in_stock: true,
                pages: 374,
                publisher: "Scholastic"
            },
            {
                title: "The Girl on the Train",
                author: "Paula Hawkins",
                genre: "Mystery",
                published_year: 2015,
                price: 14.99,
                in_stock: true,
                pages: 336,
                publisher: "Riverhead Books"
            },
            {
                title: "Educated",
                author: "Tara Westover",
                genre: "Memoir",
                published_year: 2018,
                price: 16.99,
                in_stock: true,
                pages: 334,
                publisher: "Random House"
            }
        ];
        
        const result = await books.insertMany(bookData);
        console.log(`${result.insertedCount} books inserted successfully`);
        
        // Verify insertion
        const count = await books.countDocuments();
        console.log(`Total books in collection: ${count}`);
        
    } catch (error) {
        console.error("Error inserting books:", error);
    } finally {
        await client.close();
    }
}

insertBooks().catch(console.error);

const { MongoClient } = require('mongodb');


// Define the BookstoreQueries class
class BookstoreQueries {
    constructor() {
        this.uri = "mongodb://localhost:27017";
        this.client = new MongoClient(this.uri);
        this.database = null;
        this.books = null;
    }

    async connect() {
        try {
            await this.client.connect();
            console.log("Connected to MongoDB");
            this.database = this.client.db("plp_bookstore");
            this.books = this.database.collection("books");
        } catch (error) {
            console.error("Connection error:", error);
            throw error;
        }
    }

    async disconnect() {
        await this.client.close();
        console.log("Disconnected from MongoDB");
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
        console.log("Creating indexes...");
        
        // Single index on title
        await this.books.createIndex({ title: 1 });
        console.log("Created index on title field");
        
        // Compound index on author and published_year
        await this.books.createIndex({ author: 1, published_year: -1 });
        console.log("Created compound index on author and published_year");
        
        // Verify indexes
        const indexes = await this.books.indexes();
        console.log("Current indexes:", indexes.map(idx => idx.name));
    }

    async demonstrateIndexPerformance() {
        console.log("\n=== Performance Demonstration ===");
        
        // Without index (on a non-indexed field for comparison)
        console.log("Query without index (genre):");
        const explainGenre = await this.books.find({ genre: "Fiction" }).explain();
        console.log(`Documents examined: ${explainGenre.executionStats.totalDocsExamined}`);
        console.log(`Execution time: ${explainGenre.executionStats.executionTimeMillis}ms`);
        
        // With index (title)
        console.log("\nQuery with index (title):");
        const explainTitle = await this.books.find({ title: "The Great Gatsby" }).explain();
        console.log(`Documents examined: ${explainTitle.executionStats.totalDocsExamined}`);
        console.log(`Execution time: ${explainTitle.executionStats.executionTimeMillis}ms`);
        
        // With compound index
        console.log("\nQuery with compound index (author and year):");
        const explainAuthorYear = await this.books.find({ 
            author: "J.K. Rowling", 
            published_year: { $gt: 1990 } 
        }).explain();
        console.log(`Documents examined: ${explainAuthorYear.executionStats.totalDocsExamined}`);
        console.log(`Execution time: ${explainAuthorYear.executionStats.executionTimeMillis}ms`);
    }
}

// Main execution function
async function main() {
    const bookstore = new BookstoreQueries();
    
    try {
        await bookstore.connect();
        
        console.log("=== MONGODB WEEK 1 ASSIGNMENT ===\n");
        
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
        console.error("Error in main execution:", error);
    } finally {
        await bookstore.disconnect();
    }
}

// Run if this file is executed directly
if (require.main === module) {
    main().catch(console.error);
}

module.exports = BookstoreQueries;
package main

import (
	"context"
	"database/sql"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
	_ "github.com/lib/pq"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var mongoClient *mongo.Client
var postgresDB *sql.DB

// User struct to represent the user data
type User struct {
	Name     string `json:"name" bson:"name"`
	Email    string `json:"email" bson:"email"`
	Password string `json:"password" bson:"password"`
}

// Product struct to represent the product data
type Product struct {
	Name        string  `json:"name" bson:"name"`
	Price       float64 `json:"price" bson:"price"`
	Description string  `json:"description" bson:"description"`
}

// MongoDB connection
func connectMongoDB() (*mongo.Client, error) {
	clientOptions := options.Client().ApplyURI("mongodb+srv://cutiehari:e9tnKNSptSSxadn2@cluster0.6somgn7.mongodb.net/?retryWrites=true&w=majority")
	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		return nil, err
	}
	err = client.Ping(context.Background(), nil)
	if err != nil {
		return nil, err
	}
	return client, nil
}

// PostgreSQL connection
func connectPostgresDB() (*sql.DB, error) {
	connStr := "host=localhost port=5432 user=postgres password=root dbname=migration sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}
	return db, nil
}

// Create PostgreSQL tables if not exists
func migratePostgresDB(db *sql.DB) error {
	// Create users table
	query := `
    CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT NOT NULL UNIQUE,
        password TEXT NOT NULL
    );`
	_, err := db.Exec(query)
	if err != nil {
		return err
	}
	// Create products table
	query = `
    CREATE TABLE IF NOT EXISTS products (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        price FLOAT NOT NULL,
        description TEXT
    );`
	_, err = db.Exec(query)
	return err
}

// Migrate data from MongoDB to PostgreSQL
func migrateDataFromMongoToPostgres() error {
	// Migrate users from MongoDB to PostgreSQL
	usersCollection := mongoClient.Database("migrationgo").Collection("users")
	cursor, err := usersCollection.Find(context.Background(), bson.M{})
	if err != nil {
		return err
	}
	defer cursor.Close(context.Background())
	for cursor.Next(context.Background()) {
		var user User
		if err := cursor.Decode(&user); err != nil {
			log.Printf("Error decoding MongoDB record: %v", err)
			continue
		}
		// Insert user into PostgreSQL
		query := `INSERT INTO users (name, email, password) VALUES ($1, $2, $3) ON CONFLICT (email) DO NOTHING`
		_, err := postgresDB.Exec(query, user.Name, user.Email, user.Password)
		if err != nil {
			log.Printf("Error inserting into PostgreSQL: %v", err)
		}
	}
	// Migrate products from MongoDB to PostgreSQL
	productsCollection := mongoClient.Database("migrationgo").Collection("products")
	cursor, err = productsCollection.Find(context.Background(), bson.M{})
	if err != nil {
		return err
	}
	defer cursor.Close(context.Background())
	for cursor.Next(context.Background()) {
		var product Product
		if err := cursor.Decode(&product); err != nil {
			log.Printf("Error decoding MongoDB record: %v", err)
			continue
		}
		// Insert product into PostgreSQL
		query := `INSERT INTO products (name, price, description) VALUES ($1, $2, $3)`
		_, err := postgresDB.Exec(query, product.Name, product.Price, product.Description)
		if err != nil {
			log.Printf("Error inserting into PostgreSQL: %v", err)
		}
	}
	return nil
}

// CreateUser handler to create a new user
func createUser(c *gin.Context) {
	var user User
	// Bind JSON to User struct
	if err := c.ShouldBindJSON(&user); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	// Insert user into MongoDB
	mongoCollection := mongoClient.Database("migrationgo").Collection("users")
	_, err := mongoCollection.InsertOne(context.Background(), user)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	// Insert user into PostgreSQL
	query := `INSERT INTO users (name, email, password) VALUES ($1, $2, $3)`
	_, err = postgresDB.Exec(query, user.Name, user.Email, user.Password)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to insert into PostgreSQL: " + err.Error()})
		return
	}
	// Return success response
	c.JSON(http.StatusOK, gin.H{"message": "User created successfully in MongoDB and PostgreSQL!"})
}

// CreateProduct handler to create a new product
func createProduct(c *gin.Context) {
	var product Product
	// Bind JSON to Product struct
	if err := c.ShouldBindJSON(&product); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	// Insert product into MongoDB
	mongoCollection := mongoClient.Database("migrationgo").Collection("products")
	_, err := mongoCollection.InsertOne(context.Background(), product)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	// Insert product into PostgreSQL
	query := `INSERT INTO products (name, price, description) VALUES ($1, $2, $3)`
	_, err = postgresDB.Exec(query, product.Name, product.Price, product.Description)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to insert into PostgreSQL: " + err.Error()})
		return
	}
	// Return success response
	c.JSON(http.StatusOK, gin.H{"message": "Product created successfully in MongoDB and PostgreSQL!"})
}
func main() {
	var err error
	// Connect to MongoDB
	mongoClient, err = connectMongoDB()
	if err != nil {
		log.Fatalf("Failed to connect to MongoDB: %v", err)
	}
	// Connect to PostgreSQL
	postgresDB, err = connectPostgresDB()
	if err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer postgresDB.Close()
	// Run migrations for PostgreSQL
	err = migratePostgresDB(postgresDB)
	if err != nil {
		log.Fatalf("Failed to migrate PostgreSQL database: %v", err)
	}
	// Migrate data from MongoDB to PostgreSQL
	err = migrateDataFromMongoToPostgres()
	if err != nil {
		log.Fatalf("Failed to migrate data from MongoDB to PostgreSQL: %v", err)
	}
	// Set up Gin router
	router := gin.Default()
	// Define the routes
	router.POST("/users", createUser)
	router.POST("/products", createProduct)
	// Start the server
	router.Run(":8080")
}

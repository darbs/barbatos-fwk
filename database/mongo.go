package database

import (
	"fmt"
	"sync"

	// TODO use mongodb official lib once its released
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
)

var (
	singleton *Database
	once      sync.Once
	dbName    string
	endpoint  string
)

// Singleton database instance
type Database struct {
	session *mgo.Session
}

// Table interface that wraps mgo collections
type Table struct {
	collection *mgo.Collection
}

// Query interface that wraps mgg functionality
type Query map[string]interface{}

// Note go vendoring is kind of weird
// need to hide this from any other package that
// vendors the fwk lib else types dont match up
type Index struct {
	Key        []string // Index key fields; prefix name with dash (-) for descending order
	Unique     bool     // Prevent two documents from having the same index key
	Dups       bool     // Drop documents with the same index key as a previously indexed one
	Background bool     // Build index in background and return immediately
	Sparse     bool     // Only index documents containing the Key fields
}

// Configure static db instance
func Configure(url string, name string) {
	endpoint = url
	dbName = name
}

// Get database connection
func GetDatabase() (*Database) {
	if endpoint == "" {
		panic(fmt.Errorf("no db endpoint defined"))
	}

	if dbName == "" {
		panic(fmt.Errorf("no db name defined"))
	}

	once.Do(func() {
		session, err := mgo.Dial(endpoint)
		if err != nil {
			panic(fmt.Errorf("Failed to connect to mongo 0database: %v", err))
		}

		// TODO decide on a mode
		//session.SetMode(mgo.Monotonic, true)

		singleton = &Database{session: session}
	})

	return singleton
}

// Get new object Id
func GetNewObjectId () string {
	return bson.NewObjectId().String()
}

//////////////////////////////
// Vendor related wrappings //
//////////////////////////////

// Get/Create collection
func (d Database) Table(tableName string) *Table {
	return &Table{collection: d.session.DB(dbName).C(tableName)}
}

// Ensure index on collection
func (t Table) Index(index Index) error {
	return t.collection.EnsureIndex(mgo.Index{
		Key:        index.Key,
		Unique:     index.Unique,
		DropDups:   !index.Dups,
		Background: index.Background,
		Sparse:     index.Sparse,
	})
}

// Empty collection
func (t Table) Empty() error {
	return t.collection.DropCollection()
}

// Insert item into Table
func (t Table) Insert(document ...interface{}) error {
	return t.collection.Insert(document...)
}

// Find item from Table
func (t Table) Find(params interface{}, result interface{}, limit int) (error) {
	query := t.collection.Find(params)
	if limit > 0 {
		query.Limit(limit)
	}

	err := query.Iter().All(result)
	if err != nil {
		return err
	}

	return nil
}

// TODO bulk insert
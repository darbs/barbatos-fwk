package database

import (
	"fmt"
	"log"
	"sync"

	// TODO use mongodb official lib once its released
	"github.com/globalsign/mgo"
)

var (
	singleton *database
	once      sync.Once
	dbName    string
	endpoint  string
)

type database struct {
	session  *mgo.Session
}

// Note go vendoring is kind of weird
// need to hide this from any other package that
// vendors the fwk lib else types dont match up
type Collection *mgo.Collection
type Index struct {
	Key        []string // Index key fields; prefix name with dash (-) for descending order
	Unique     bool     // Prevent two documents from having the same index key
	Dups       bool     // Drop documents with the same index key as a previously indexed one
	Background bool     // Build index in background and return immediately
	Sparse     bool     // Only index documents containing the Key fields
}

func Configure(url string, name string) {
	endpoint = url
	dbName = name
}

func Database() (*database) {
	if endpoint == "" {
		panic(fmt.Errorf("no db endpoint defined"))
	}

	if dbName == "" {
		panic(fmt.Errorf("no db name defined"))
	}

	once.Do(func() {
		session, err := mgo.Dial(endpoint)
		if err != nil {
			log.Println("failed to connect to db")
			panic(err)
		}

		// TODO decide on a mode
		//session.SetMode(mgo.Monotonic, true)

		singleton = &database{session: session}
	})

	return singleton
}

func (d database) Table(table string) *mgo.Collection {
	return d.session.DB(dbName).C(table)
}

//////////////////////////////
// Vendor related wrappings //
//////////////////////////////

func (d database) Index(table string, index Index) error {
	return d.Table(table).EnsureIndex(mgo.Index{
		Key:        index.Key,
		Unique:     index.Unique,
		DropDups:   !index.Dups,
		Background: index.Background,
		Sparse:     index.Sparse,
	})
}

func (d database) Insert(table string, document ...interface{}) error {
	return d.Table(table).Insert(document...)
}

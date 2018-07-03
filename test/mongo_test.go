package test

import (
	"log"
	"os"
	"strconv"
	"testing"
	"math/rand"

	"github.com/darbs/barbatos-fwk/config"
	"github.com/darbs/barbatos-fwk/database"
)

var (
	tableName = "TestTable"
	testId = strconv.Itoa(rand.Int())
)

type generic struct {
	Attr        string  `json:"attr" bson:"attr"`
}

func TestDbConnectFail(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Db did not exit when invalid config")
		}
	}()

	database.Configure("", "")
	database.GetDatabase()
}

func TestDbCreateIndex(t *testing.T) {
	conf := config.GetConfig()
	database.Configure(conf.DbEndpoint, conf.DbName)
	db := database.GetDatabase()
	table := db.Table(tableName)


	// Index
	index := database.Index{
		Key:        []string{"TestIndex"},
		Unique:     true,
		Dups:       false,
		Background: true,
		Sparse:     true,
	}

	err := table.Index(index)
	if err != nil {
		t.Errorf("Db failed to create index")
	}
}

func TestDbInsertOne(t *testing.T) {
	conf := config.GetConfig()
	database.Configure(conf.DbEndpoint, conf.DbName)
	db := database.GetDatabase()
	table := db.Table(tableName)

	gen := generic{testId}
	err := table.Insert(gen)
	if err != nil {
		t.Errorf("Db failed to insert")
	}
}

func TestDbInsertMany(t *testing.T) {
	conf := config.GetConfig()
	database.Configure(conf.DbEndpoint, conf.DbName)
	db := database.GetDatabase()
	table := db.Table(tableName)

	err := table.Insert(generic{"123"}, generic{"456"})

	if err != nil {
		t.Errorf("Db failed to insert many")
		log.Printf("ERROR: %v", err)
	}
}


func TestDbFindOne(t *testing.T) {
	conf := config.GetConfig()
	database.Configure(conf.DbEndpoint, conf.DbName)
	db := database.GetDatabase()
	table := db.Table(tableName)

	var res []generic
	err := table.Find(generic{Attr: testId}, &res, -1)
	if err != nil {
		t.Errorf("Db failed to find document")
	}

	if len(res) != 1 {
		t.Errorf("Db failed to find matching id document")
	}
}

func TestDbFindAll(t *testing.T) {
	conf := config.GetConfig()
	database.Configure(conf.DbEndpoint, conf.DbName)
	db := database.GetDatabase()
	table := db.Table(tableName)

	gen := generic{testId}
	err := table.Insert(gen)

	var res []generic
	err = table.Find(nil, &res, -1)
	if err != nil {
		t.Errorf("Db failed to find document")
	}

	if len(res) != 4 {
		log.Printf("%v", res)
		t.Errorf("Db failed to find all documents")
	}
}

func TestDbEmpty(t *testing.T) {
	conf := config.GetConfig()
	database.Configure(conf.DbEndpoint, conf.DbName)
	db := database.GetDatabase()

	table := db.Table(tableName)

	err := table.Empty()
	if err != nil {
		t.Errorf("Db error occured when trying to empty collection")
	}

	var res []generic
	err = table.Find(nil, &res, -1)
	if len(res) != 0 {
		t.Errorf("Db failed to empty collection")
	}
}

func setup () {
	conf := config.GetConfig()
	database.Configure(conf.DbEndpoint, conf.DbName)
	db := database.GetDatabase()
	table := db.Table(tableName)

	table.Empty()
}

func TestMain(m *testing.M) {
	setup()
	log.Println("TestMain")
	code := m.Run()
	//shutdown()
	os.Exit(code)
}
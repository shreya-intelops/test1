package sqls

import (
	"database/sql"
	"errors"
	_ "github.com/mattn/go-sqlite3"
	log "github.com/sirupsen/logrus"
	"github.com/uptrace/opentelemetry-go-extra/otelsql"
	semconv "go.opentelemetry.io/otel/semconv/v1.20.0"
	"os"
	"sync"
)

var (
	ErrDuplicate    = errors.New("record already exists")
	ErrNotExists    = errors.New("row not exists")
	ErrUpdateFailed = errors.New("update failed")
	ErrDeleteFailed = errors.New("delete failed")
)

var o sync.Once

const FileName = "rest-sqlite.db"

type SQLiteClient struct {
	DB *sql.DB
}

var err error
var sqliteClient *SQLiteClient

func InitSqliteDB() (*SQLiteClient, error) {
	o.Do(func() {
		if _, err = os.Stat(FileName); err == nil {
			err = os.Remove(FileName)
			if err != nil {
				log.Debugf("unable to remove database file, %v", err)
				os.Exit(1)
			}
		}

		var db *sql.DB
		serviceName := os.Getenv("SERVICE_NAME")
		collectorURL := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
		if len(serviceName) > 0 && len(collectorURL) > 0 {
			// add opentel
			db, err = otelsql.Open("sqlite3", FileName, otelsql.WithAttributes(semconv.DBSystemSqlite))
		} else {
			db, err = sql.Open("sqlite3", FileName)
		}
		if err != nil {
			log.Debugf("database connection error, %v", err)
			os.Exit(1)
		}
		sqliteClient = &SQLiteClient{
			DB: db,
		}

	})

	return sqliteClient, err
}

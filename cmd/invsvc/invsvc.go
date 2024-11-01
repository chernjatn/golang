package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"main/internal/dbinv"
	"main/internal/invsvc"
	"main/internal/redisinv"
	"os"
	"strconv"

	"github.com/go-redis/redis/v8"
	"github.com/go-sql-driver/mysql"
)

type ConfigType struct {
	Port  uint
	Redis *redis.Options
	Db    *mysql.Config
}

var config *ConfigType

func init() {
	redisDB, err := strconv.Atoi("4")
	if err != nil {
		log.Fatal("Env not valid")
	}
	if redisDB < 0 {
		log.Fatal("Env not found redis db:", redisDB)
	}

	config = &ConfigType{
		Port: 8000,
		Redis: &redis.Options{
			Addr:     os.Getenv("REDIS_HOST"),
			Password: "",
			DB:       redisDB,
		},
		Db: &mysql.Config{
			Net:                  "tcp",
			AllowNativePasswords: true,
			User:                 os.Getenv("DB_USERNAME"),
			Passwd:               os.Getenv("DB_PASSWORD"),
			Addr:                 os.Getenv("DB_HOST"),
			DBName:               os.Getenv("DB_DATABASE"),
			ParseTime:            true,
			MultiStatements:      true,
		},
	}
}

func main() {
	var ctx = context.Background()

	db, err := sql.Open("mysql", config.Db.FormatDSN())
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	db.SetMaxIdleConns(100)
	db.SetMaxOpenConns(1000)

	rdb := redis.NewClient(config.Redis)
	defer rdb.Close()

	rep := redisinv.GetRepository(&ctx, rdb)
	dbRep := dbinv.GetRepository(&ctx, db)

	if err := invsvc.StartServer(dbRep, rep, fmt.Sprintf(":%d", config.Port)); err != nil {
		panic(err)
	}
}

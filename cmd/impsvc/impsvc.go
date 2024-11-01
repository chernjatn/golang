package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"main/internal/dbinv"
	"main/internal/ecom"
	"main/internal/importService"
	"main/internal/redisinv"
	"os"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/go-sql-driver/mysql"
)

type ConfigType struct {
	Ecom                  *ecom.EcomConfig
	Redis                 *redis.Options
	Db                    *mysql.Config
	ShopFlushUrl          string
	ImportIntervalMinutes int
}

var config *ConfigType

func init() {

	redisDB, err := strconv.Atoi(os.Getenv("INVENTORY_DB"))
	if err != nil {
		log.Fatal("Env not valid INVENTORY_DB")
	}

	shopFlushUrl := os.Getenv("SHOP_FLUSHURL")
	if shopFlushUrl == "" {
		log.Fatal("Env not valid SHOP_FLUSHURL")
	}

	importIntervalMinutes, err := strconv.Atoi(os.Getenv("IMPORT_INTERVAL_MINUTES"))
	if err != nil {
		log.Fatal("Env not valid IMPORT_INTERVAL_MINUTES")
	}

	config = &ConfigType{
		ShopFlushUrl: shopFlushUrl,
		ImportIntervalMinutes: importIntervalMinutes,
	}

	config.Redis = &redis.Options{
		Addr:         os.Getenv("REDIS_HOST"),
		Password:     "",
		DB:           redisDB,
		PoolSize:     100,
		ReadTimeout:  1 * time.Minute,
		PoolTimeout:  2 * time.Minute,
	}

	config.Ecom = &ecom.EcomConfig{
		User:     os.Getenv("ECOM_USER"),
		Password: os.Getenv("ECOM_PASSWORD"),
		Host:     os.Getenv("ECOM_HOST"),
	}

	config.Db = &mysql.Config{
		Net:                  "tcp",
		AllowNativePasswords: true,
		User:                 os.Getenv("DB_USERNAME"),
		Passwd:               os.Getenv("DB_PASSWORD"),
		Addr:                 os.Getenv("DB_HOST"),
		DBName:               os.Getenv("DB_DATABASE"),
		ParseTime:            true,
		MultiStatements:      true,
	}
}

func importInv(ctx *context.Context) {
	ecom := ecom.GetClient(ctx, *config.Ecom)

	db, err := sql.Open("mysql", config.Db.FormatDSN())
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	db.SetMaxIdleConns(100)
	db.SetMaxOpenConns(1000)

	rdb := redis.NewClient(config.Redis)
	defer rdb.Close()

	rRep := redisinv.GetRepository(ctx, rdb)

	//новый инстанс на каждый импорт, т.к есть кеш
	dbRep := dbinv.GetRepository(ctx, db)

	is := importService.GetService(dbRep, ecom, rRep, config.ShopFlushUrl)

	is.ImportInventory(ctx)
}

func main() {
	for {
		var ctx = context.Background()

		fmt.Println("start")

		importInv(&ctx)

		fmt.Println("fin")

		<-time.After(time.Duration(config.ImportIntervalMinutes) * time.Minute)
	}
}

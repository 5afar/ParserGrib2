// Пакет создает пул соединений для базы данных PostgreSQL и проверяет наличие необходимых таблиц
//
//
package database

import (
	"context"
	"fmt"
	"gribV2.com/config"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Пул соединений в PostgreSQL
var Dbpool *pgxpool.Pool

// Create Инициализирует пул соединений и выполняет создание таблиц, если их не существует
func Create() {
	var err error
	cfg := config.New()
	strconn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s pool_max_conns=100", cfg.PGHost, cfg.PGPort, cfg.PGUser, cfg.PGPass, cfg.PGBase)
	Dbpool, err = pgxpool.New(context.Background(), strconn)
	if err != nil {
		config.Logger.WithError(err).Error("Ошибка подключения")
		os.Exit(11)
	}

	if Dbpool == nil{
		config.Logger.Error("Не удалось создать пул подключений, проверьте правилность данных для авторизации!")
		os.Exit(11)
	}

	migrateGribData()
	migrateHash()

}

// migrateGribData Создает таблицы для данных
func migrateGribData() {
	createTableSQL := `CREATE TABLE IF NOT EXISTS grib_data
	(
		id uuid NOT NULL,
		grib_datetime timestamp without time zone,
		forecast_time integer,
		parameter text COLLATE pg_catalog."default",
		surface_type text COLLATE pg_catalog."default",
		surface_value text COLLATE pg_catalog."default",
		grid_properties json,
		grib_data double precision[],
		grib_data_int integer[],
		CONSTRAINT grib_data_pkey PRIMARY KEY (id)
	)`

	conn, err := Dbpool.Acquire(context.Background())
	if err != nil {
		config.Logger.WithError(err).Error("Ошибка соединения с БД при создании таблицы")
		os.Exit(11)
	}
	_, err = conn.Exec(context.Background(), createTableSQL)
	if err != nil {
		config.Logger.WithError(err).Error("Ошибка выполнения запроса создания таблицы")
		conn.Release()
		os.Exit(11)
	}
	createTableSQL = `CREATE TABLE IF NOT EXISTS grib_data_buff
	(
		id uuid NOT NULL,
		grib_datetime timestamp without time zone,
		forecast_time integer,
		parameter text COLLATE pg_catalog."default",
		surface_type text COLLATE pg_catalog."default",
		surface_value text COLLATE pg_catalog."default",
		grid_properties json,
		grib_data double precision[],
		grib_data_int integer[],
		CONSTRAINT grib_data_buff_pkey PRIMARY KEY (id)
	)`

	_, err = conn.Exec(context.Background(), createTableSQL)
	if err != nil {
		config.Logger.WithError(err).Error("Ошибка выполнения запроса создания таблицы")
		conn.Release()
		os.Exit(11)
	}
	config.Logger.Info("Таблицы готова к работе!")
	conn.Release()
}

// migrateHash Создает таблицу, в которой хранятся хеш-суммы прочитанных файлов
func migrateHash() {
	tableName := "hashes"

	createTableSQL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s
	(
		grib_hash character varying(256) NOT NULL,
		CONSTRAINT hashes_pkey PRIMARY KEY (grib_hash)
	)`, tableName)

	conn, err := Dbpool.Acquire(context.Background())
	if err != nil {
		config.Logger.WithError(err).Error("Ошибка соединения с БД при создании таблицы")
		os.Exit(11)
	}
	_, err = conn.Exec(context.Background(), createTableSQL)
	if err != nil {
		config.Logger.WithError(err).Error("Ошибка выполнения запроса создания таблицы")
		conn.Release()
		os.Exit(11)
	}
	config.Logger.Info("Таблица готова к работе!")
	conn.Release()
}


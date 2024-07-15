// Пакет для получения соединения с clickhouse и создания необходимых таблиц
//
// Создает соединение с ClickHouse по указанному в .env-фйале ip-адресу и порту,
// с заданным пользователем и к необходимой базе данных.
// Создает таблицы для заполнения их данными
package clickhouse

import (
	"context"
	"fmt"
	"gribV2.com/config"
	"log"
	"net"
	"os"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// GetConn Создает и возвращает соединение с clickhouse
func GetConn(cfg *config.Config) (driver.Conn, error) {
	dialCount := 0
	addr := fmt.Sprintf("%s:%s", cfg.CHHost, cfg.CHPort)
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: cfg.CHBase,
			Username: cfg.CHUser,
			Password: cfg.CHPass,
		},
		DialContext: func(ctx context.Context, addr string) (net.Conn, error) {
			dialCount++
			var d net.Dialer
			return d.DialContext(ctx, "tcp", addr)
		},
		// Отладочную информацию можно включить изменив переменную Debug на true(использовать в случае ошибок с clickhouse)
		Debug: true,
		Debugf: func(format string, v ...any) {
			f, err := os.OpenFile("clickhouse_log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
			if err != nil {
				log.Fatalf("error opening file: %v", err)
			}
			defer f.Close()
			log.SetOutput(f)
			log.Printf(format, v)
		},
		Settings: clickhouse.Settings{
			// Увеличивает допустимое время выполнения запроса
			"max_execution_time": 1200,
			// Разрешает использовать эксперементальные типы данных (необходимо для сохранения в json)
			"allow_experimental_object_type": 1,
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		DialTimeout:          time.Second * 30,
		MaxOpenConns:         100,
		MaxIdleConns:         5,
		ConnMaxLifetime:      30 * time.Minute,
		ConnOpenStrategy:     clickhouse.ConnOpenInOrder,
		BlockBufferSize:      255,
		MaxCompressionBuffer: 10485760,
	})

	if err != nil {
		return nil, err
	}
	err = conn.Ping(context.Background())
	if err != nil {
		return nil, err
	}
	return conn, err
}

// CheckTable Проверяет, существуют ли необходимые таблицы, и, если не существуют, создает их
func CheckTable(cfg *config.Config) error {
	// Полученеи соединения
	clickhouseConn, err := GetConn(cfg)
	if err != nil {
		return err
	}
	// Строка запроса на создание таблицы grib_data, в которой хранятся массивы
	q_gribData := `CREATE TABLE IF NOT EXISTS grib_data
	(
		id UUID,
	
		grib_data Array(Float64),

		grib_data_int Array(Int32),
	
		data_index Int32
	)
	ENGINE = MergeTree
	ORDER BY (id, data_index)
	SETTINGS index_granularity = 8192;`
	// Строка запроса на создание буферной таблицы для хранения данных во время смены среза
	q_gribDataBuff := `CREATE TABLE IF NOT EXISTS grib_data_buff
	(
		id UUID,
	
		grib_data Array(Float64),

		grib_data_int Array(Int32),
	
		data_index Int32
	)
	ENGINE = MergeTree
	ORDER BY (id, data_index)
	SETTINGS index_granularity = 8192;`
	// Строка запроса на создание таблицы, в которой хранятся данные предыдущего среза
	q_gribDataPrev := `CREATE TABLE IF NOT EXISTS grib_data_prev
	(
		id UUID,
	
		grib_data Array(Float64),

		grib_data_int Array(Int32),
	
		data_index Int32
	)
	ENGINE = MergeTree
	ORDER BY (id, data_index)
	SETTINGS index_granularity = 8192;`
	// Строка запроса на создание таблицы, в которой хранятся свойства данных
	q_grid := `CREATE TABLE IF NOT EXISTS grid
	(
		id UUID,

		grib_datetime DateTime,
	
		forecast_time Int32,
	
		parameter String,
	
		surface_value String,
	
		surface_type String,

		grid JSON
	)
	ENGINE = MergeTree
	ORDER BY (surface_value, parameter)
	SETTINGS index_granularity = 8192;`
	// Строка запроса на создание буферной таблицы, в которой хранятся свойства данных
	q_grid_buff := `CREATE TABLE IF NOT EXISTS grid_buff
	(
		id UUID,

		grib_datetime DateTime,

		forecast_time Int32,

		parameter String,

		surface_value String,

		surface_type String,

		grid JSON
	)
	ENGINE = MergeTree
	ORDER BY (surface_value, parameter)
	SETTINGS index_granularity = 8192;`
	// Строка запроса на создание таблицы, в которой хранятся предыдущие свойства данных
	q_grid_prev := `CREATE TABLE IF NOT EXISTS grid_prev
	(
		id UUID,

		grib_datetime DateTime,

		forecast_time Int32,

		parameter String,

		surface_value String,

		surface_type String,

		grid JSON
	)
	ENGINE = MergeTree
	ORDER BY (surface_value, parameter)
	SETTINGS index_granularity = 8192;`
	q_file := `CREATE TABLE IF NOT EXISTS file_name
	(
		file_name String
	)
	ENGINE = MergeTree()
	ORDER BY file_name;`

	q_viewData := `CREATE MATERIALIZED VIEW IF NOT EXISTS view_data
	(
	
		id UUID,
	
		grib_data Array(Float64),
	
		data_index Int32
	)
	ENGINE = MergeTree
	ORDER BY (data_index, id)
	AS SELECT id, grib_data, data_index
	FROM grib_data`
	q_viewPrev := `CREATE MATERIALIZED VIEW IF NOT EXISTS view_prev
	(
	
		id UUID,
	
		grib_data Array(Float64),
	
		data_index Int32
	)
	ENGINE = MergeTree
	ORDER BY (data_index, id)
	AS SELECT id, grib_data, data_index
	FROM grib_data_prev`
	q_viewBuff := `CREATE MATERIALIZED VIEW IF NOT EXISTS view_buff
	(
	
		id UUID,
	
		grib_data Array(Float64),
	
		data_index Int32
	)
	ENGINE = MergeTree
	ORDER BY (data_index, id)
	AS SELECT id, grib_data, data_index
	FROM grib_data_buff`

	// Блок выполнения запросов на создание
	err = clickhouseConn.Exec(context.Background(), q_gribData)
	if err != nil {
		return err
	}
	err = clickhouseConn.Exec(context.Background(), q_viewData)
	if err != nil {
		return err
	}

	err = clickhouseConn.Exec(context.Background(), q_gribDataBuff)
	if err != nil {
		return err
	}
	err = clickhouseConn.Exec(context.Background(), q_viewBuff)
	if err != nil {
		return err
	}

	err = clickhouseConn.Exec(context.Background(), q_gribDataPrev)
	if err != nil {
		return err
	}
	err = clickhouseConn.Exec(context.Background(), q_viewPrev)
	if err != nil {
		return err
	}

	err = clickhouseConn.Exec(context.Background(), q_grid)
	if err != nil {
		return err
	}
	err = clickhouseConn.Exec(context.Background(), q_grid_buff)
	if err != nil {
		return err
	}
	err = clickhouseConn.Exec(context.Background(), q_grid_prev)
	if err != nil {
		return err
	}

	err = clickhouseConn.Exec(context.Background(), q_file)
	if err != nil {
		return err
	}

	defer clickhouseConn.Close()

	return nil
}

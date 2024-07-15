package grib2

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	ch "gribV2.com/clickhouse"
	"gribV2.com/config"
	"time"
)

// Размер отправляемой пачки в ClickHouse
const (
	BATCH_SIZE = 50000
	CHUNK_SIZE = 1600
)

// chunkIntSlice нарезает большой массив данных на более маленькие для лучшей отправки и доступа к данным из БД
func chunkIntSlice(slice []int, chunkSize int) [][]int {
	var chunks [][]int
	for i := 0; i < len(slice); i += chunkSize {
		end := i + chunkSize
		if end > len(slice) {
			end = len(slice)
		}
		chunks = append(chunks, slice[i:end])
	}
	return chunks
}

// chunkFloat64Slice нарезает большой массив данных на более маленькие для лучшей отправки и доступа к данным из БД
func chunkFloat64Slice(slice []float64, chunkSize int) [][]float64 {
	var chunks [][]float64
	for i := 0; i < len(slice); i += chunkSize {
		end := i + chunkSize
		if end > len(slice) {
			end = len(slice)
		}
		chunks = append(chunks, slice[i:end])
	}
	return chunks
}

// ExportBatch формирует пачки согласно BATCH_SIZE и отправляет их в ClickHouse по готовности
func ExportBatch(bufChannel chan *Table, cfg *config.Config) error {

	currTime := time.Now()
	hour := currTime.Hour()
	// Получение соединения
	clickhouseConn, err := ch.GetConn(cfg)
	if err != nil {
		config.Logger.WithError(err)
		return err
	}
	// Получение количества файлов для определения таблицы, в которую будет идти запись
	var count_file uint64
	if err := clickhouseConn.QueryRow(context.Background(), "SELECT COUNT(*) FROM file_name").Scan(&count_file); err != nil {
		config.Logger.Warn("Ошибка количества файлов!")
		return err
	}
	config.Logger.Info("Количество записанных файлов: ",count_file)

	// Проверка времени и количества файлов для определения таблицы
	var q_grid string
	if count_file < 42 {
		if hour >= 1 && hour < 12 {
			tableName = "grib_data"
			q_grid = "INSERT INTO grid (id, grib_datetime, forecast_time, parameter, surface_type, surface_value, grid) VALUES(?,?,?,?,?,?,?)"
		} else if hour == 12 || hour == 0 {
			tableName = "grib_data_buff"
			q_grid = "INSERT INTO grid_buff (id, grib_datetime, forecast_time, parameter, surface_type, surface_value, grid) VALUES(?,?,?,?,?,?,?)"
		} else if hour >= 13 && hour < 24 {
			tableName = "grib_data"
			q_grid = "INSERT INTO grid (id, grib_datetime, forecast_time, parameter, surface_type, surface_value, grid) VALUES(?,?,?,?,?,?,?)"
		} else {
			panic("Проблема с определением времени!")
		}
	} else {
		q_grid = "INSERT INTO grid (id, grib_datetime, forecast_time, parameter, surface_type, surface_value, grid) VALUES(?,?,?,?,?,?,?)"
		tableName="grib_data_buff"
	}


	// Текущий размер пачки
	count := 0
	// Строка запроса в ClickHouse на вставку данных
	q := fmt.Sprintf("INSERT INTO %s", tableName)
	// Подготовка пачки
	batch, err := clickhouseConn.PrepareBatch(context.Background(), q)
	if err != nil {
		config.Logger.WithError(err).Warn("Ошибка подготовки пачки!(0)")
		return err
	}
	// Цикл получающий из канала прочитанные сообщения и формирующий из них пачки на отправку
	for {
		// Если текущий размер пачки больше или равен BATCH_SIZE, то пачка отправляется в БД
		if count >= BATCH_SIZE {
			config.Logger.Info("Отправка... (", count, ")")
			err := batch.Send()
			// Если во время отправки данных появляется ошибка, то пытается повторить 10 раз
			if err != nil {
				// flag := false
				// for i := 0; i < 10; i++ {
				for {
					err := batch.Send()
					if err == nil {
						// flag = true
						break
					}
				}
				// Если отправка так и не удалась, то закрывает соединение и возвращает ошибку
				// if !flag {
				// 	clickhouseConn.Close()
				// 	config.Logger.Warn("Ошибка отправки пачки!")
				// 	return err
				// }
			}
			// Пересоздание соединения после отправки пачки для обновления времени жизни соединения
			clickhouseConn.Close()
			clickhouseConn, err = ch.GetConn(cfg)
			if err != nil {
				config.Logger.WithError(err)
				return err
			}
			batch, err = clickhouseConn.PrepareBatch(context.Background(), q)
			if err != nil {
				clickhouseConn.Close()
				config.Logger.WithError(err).Warn("Ошибка подготовки пачки!(1)")
				return err
			}
			count = 0
			continue
		}

		// Если канал закрыт и пуст, отправляется последняя пачка и закрывается соединение
		// Иначе читается следущее сообщение из канала и готовится к отправке
		if item, ok := <-bufChannel; !ok {
			config.Logger.Info("Канал закрыт!")
			err := batch.Send()
			if err != nil {
				for {
					err := batch.Send()
					if err == nil {
						break
					}
				}
			}
			clickhouseConn.Close()
			return nil
		} else {
			// Нарезка массивов данных на маленькие чанки
			chunkInt := chunkIntSlice(item.Data_int, CHUNK_SIZE)
			chunkFloat := chunkFloat64Slice(item.Data, CHUNK_SIZE)
			if len(chunkFloat) != len(chunkInt) {
				clickhouseConn.Close()
				return errors.New("Массивы не совпадают")
			}
			// Формирование json для описания параметров сетки
			jsonData, err := json.Marshal(item.Section3)
			if err != nil {
				return err
			}
			// Преобразование json в строку для лучшей записи
			jString := string(jsonData)
			// Запись в БД параметров сетки и даных для текущего сообщения
			err = clickhouseConn.Exec(context.Background(), q_grid,
				item.UUID,
				item.Date,
				item.ForecastTime,
				item.Param,
				item.SurfaceType,
				item.SurfaceValue,
				jString,
			)
			if err != nil {
				return err
			}
			// Заполнение пачки нарезанными чанками
			for i := 0; i < len(chunkInt); i++ {
				count++
				index := i * CHUNK_SIZE
				err := batch.Append(
					item.UUID,
					chunkFloat[i],
					chunkInt[i],
					index,
				)
				if err != nil {
					clickhouseConn.Close()
					return err
				}
			}

		}
	}
}

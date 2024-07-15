package grib2

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"gribV2.com/config"
	"gribV2.com/database"
	"io/ioutil"
	"os"
	"time"
)

// SaveMessage Сохраняет данные из GRIB2-файла в формате json по секциям
func SaveMessage(savePath string, bufChannel chan *Message) error {
	for {
		var ms *Message
		var ok bool
		if ms, ok = <-bufChannel; !ok {
			config.Logger.Info("Канал закрыт, сохранение завершено!")
			return nil
		}
		jsonData, err := json.Marshal(ms)
		if err != nil {
			config.Logger.WithError(err).Error("Ошибка формирования json")
			return err
		}
		path := savePath + "/" + fmt.Sprint(ms.Section1.ReferenceTime) + "/" + fmt.Sprint(ms.Section4.ProductDefinitionTemplate.ForecastTime)
		prefix, err := createFolder(path)
		if err != nil {
			config.Logger.WithError(err).Error("Ошибка создания директории")
			return err
		}
		filename := prefix + "/" + ReadProductDisciplineCategoryParameters(uint16(ms.Section0.Discipline), ms.Section4.ProductDefinitionTemplate.ParameterCategory, ms.Section4.ProductDefinitionTemplate.ParameterNumber) + "_" + ReadSurfaceTypesUnits(int(ms.Section4.ProductDefinitionTemplate.FirstSurface.Type)) + "_" + fmt.Sprintf("%dm", ms.Section4.ProductDefinitionTemplate.FirstSurface.Value)
		err = ioutil.WriteFile(filename+".json", jsonData, 0644)
		if err != nil {
			config.Logger.WithError(err).Error("Ошибка записи файла")
			return err
		}
	}
}

// SaveJson Сохраняет данные из GRIB2-файла в формате json по структуре базы данных
func SaveJson(savePath string, bufChannel chan *Table) error {
	for {
		var ms *Table
		var ok bool
		if ms, ok = <-bufChannel; !ok {
			config.Logger.Info("Канал закрыт, сохранение завершено!")
			return nil
		}
		jsonData, err := json.Marshal(ms)
		if err != nil {
			config.Logger.WithError(err).Error("Ошибка формирования json")
			return err
		}
		path := savePath + "/" + fmt.Sprint(ms.Date.Year(), "-", ms.Date.Month(), "-", ms.Date.Day(), "_", ms.Date.Hour(), "_", ms.Date.Minute(), "_", ms.Date.Second()) + "/" + fmt.Sprint(ms.ForecastTime)
		prefix, err := createFolder(path)
		if err != nil {
			config.Logger.WithError(err).Error("Ошибка создания директории")
			return err
		}
		filename := prefix + "/" + ms.Param + "_" + ms.SurfaceType + "_" + ms.SurfaceValue
		err = ioutil.WriteFile(filename+".json", jsonData, 0644)
		if err != nil {
			config.Logger.WithError(err).Error("Ошибка записи файла")
			return err
		}
	}
}

// createFolder Создает папки для сохранения json-файлов
func createFolder(path string) (string, error) {
	err := os.MkdirAll(path, 0644)
	if err != nil {
		config.Logger.WithError(err).Error("Ошибка доступа к директории")
		return path, err
	}
	return path, nil
}
// Определяет название таблицы, в которую будут сохранятся данные
var tableName string

// SaveDB Сохраняет расшифрованные грибы в базу данных PostgreSQL
func SaveDB(bufChannel chan *Table) error {
	columnNames := []string{"id", "grib_datetime", "forecast_time", "parameter", "surface_type", "surface_value", "grid_properties", "grib_data", "grib_data_int"}
	bc := make(chan *Table, 100)
	copySource := &MessageCopySource{
		Messages: bc,
	}
	currTime := time.Now()
	hour := currTime.Hour()
	if hour >= 1 && hour < 12 {
		tableName = "grib_data"
	} else if hour == 12 || hour == 0 {
		tableName = "grib_data_buff"
	} else if hour >= 13 && hour < 24 {
		tableName = "grib_data"
	}
	for {
		if item, ok := <-bufChannel; !ok {
			config.Logger.Info("Канал закрыт, завершение операции сохранения!")
			return nil
		} else {
			bc <- item
		}
		conn, err := database.Dbpool.Acquire(context.Background())
		if err != nil {
			config.Logger.WithError(err).Error("Ошибка открытия соединения")
			return err
		}
		_, err = conn.CopyFrom(
			context.Background(),
			pgx.Identifier{tableName},
			columnNames,
			copySource,
		)
		if err != nil {
			conn.Release()
			return err
		}
		conn.Release()
	}
}

// ---------------------------------------------------
// Table Структура сохранения данных в БД
type Table struct {
	UUID         uuid.UUID
	Date         time.Time
	ForecastTime uint32
	Param        string
	SurfaceType  string
	SurfaceValue string
	Section3     S3
	Data         []float64
	Data_int     []int
}

// Структура необходимая для потоковой записи в PostgreSQL
type MessageCopySource struct {
	Messages chan *Table
	Value    *Table
	Nex      *Table
	err      error
}

// Next Метод структуры MessageCopySource для получения следущих данных из канала и записи их в поток
func (s *MessageCopySource) Next() bool {
	// Возвращает true, если есть следующее сообщение в канале Messages
	// или false, если больше нет сообщений
	var ok bool
	select {
	case s.Nex, ok = <-s.Messages:
		s.Value = s.Nex
		return ok
	default:
		return false
	}

}

// Values Метод структуры MessageCopySource для возврата значений структуры
func (s *MessageCopySource) Values() ([]interface{}, error) {
	// Возвращает значения для текущего сообщения из канала Messages
	message := s.Value
	return []interface{}{message.UUID, message.Date, message.ForecastTime, message.Param, message.SurfaceType, message.SurfaceValue, message.Section3, message.Data, message.Data_int}, nil
}

// Err Метод структуры MessageCopySources обрабатывающий ошибки записи в поток
func (s *MessageCopySource) Err() error {
	// Возвращает ошибку, если такая имеется
	return s.err
}

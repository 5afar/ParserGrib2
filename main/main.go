// Программа Parser предназначена для чтения файлов формата GRIB2 и записи их в базу данных, либо в json-файлы
//
// Программа использует два типа баз данных в зависимости от предпочтений пользователя,
// есть возможность сохранять в json-файлы полученные сообщения
package main

import (
	ch "gribV2.com/clickhouse"
	"gribV2.com/config"
	"gribV2.com/database"
	"gribV2.com/grib2"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/joho/godotenv"
	"github.com/sirupsen/logrus"
)

// init Функция запускается до main и выполняет подгрузку файла конфигурации
func init() {
	if err := godotenv.Load(".env"); err != nil {
		config.Logger.Warn("Не найден файл среды")
		os.Exit(12)
	}
}


func main() {
	Parser()
}
// Parser выполняет предварительную подготовку программы и запускает чтение файлов
func Parser() {
	//------------------------------------------------------
	// Инициализация конфига
	start := time.Now()
	file, err := os.OpenFile("parser_log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		config.Logger.Error("Ошибка открытия файла лога: ", err)
	}
	config.LoggerStart(file)
	cfg := config.New()
	config.Logger.Info("Старт парсера...")
	// Чтение папки с файлами
	files, err := os.ReadDir(cfg.SrcDir)
	if err != nil {
		config.Logger.Warn("Ошибка чтения директории!")
		return
	}
	// Проверяет папку на пустоту
	if len(files) != 0 {
		config.Logger.Info("Файлы найдены")
		// Если тип сохранения database или clickhouse, проверяет таблицы в БД и готовит соединения
		if cfg.SaveAs == "database" {
			database.Create()
		}
		if cfg.SaveAs == "clickhouse"{
			err:=ch.CheckTable(cfg)
			if err!=nil{
				config.Logger.WithError(err).Error("Ошибка проверки таблиц в clickhouse!")
				os.Exit(1)
			}
		}
		// Запускается основная часть программы, где идет чтение файлов и сохранения указанным способом
		err:=grib2.Grib_menu(files, cfg)
		if err!=nil{
			config.Logger.WithError(err)
			os.Exit(1)
		}
		// Перемещает файлы из исходной папки в папку сохранения
		MoveFile(files, cfg.SrcDir, cfg.MoveDir)
		// Закрытие пула соединений с базой данных
		if cfg.SaveAs == "database" {
			database.Dbpool.Close()
		}
	} else {
		config.Logger.Info("Новые файлы не обнаружены...")
	}
	defer file.Close()
	end := time.Now()

	duration := end.Sub(start)
	logrus.Info("Время выполнения программы: ", duration)
	//-------------------------------------------------------
}

// MoveFile Функция, которая перемещает прочитанные файлы в точку сохранения
func MoveFile(files []fs.DirEntry, sourceDir string, destinationDir string) {
	if _, err := os.Stat(destinationDir); os.IsNotExist(err) {
		err := os.MkdirAll(destinationDir, 0755)
		if err != nil {
			config.Logger.WithError(err).Error("Ошибка создания директории")
			return
		}
	}
	for _, file := range files {
		if !file.IsDir() {
			err := os.Rename(filepath.Join(sourceDir, file.Name()), filepath.Join(destinationDir, file.Name()))
			if err != nil {
				config.Logger.WithError(err).Error("Ошибка перемещения файлов")
				return
			}
		} else {
			continue
		}
	}
}

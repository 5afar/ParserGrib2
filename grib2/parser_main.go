package grib2

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strconv"

	ch "gribV2.com/clickhouse"
	"gribV2.com/config"
	"gribV2.com/database"

	"golang.org/x/sync/errgroup"

	"github.com/jackc/pgx/v5"
)

var (
	errGroup errgroup.Group
	egg      errgroup.Group
	eg       errgroup.Group
)
var (
	SaveAs   string
	Conns    int
	CountCpu int
)

func Grib_menu(dirPath []fs.DirEntry, cfg *config.Config) error {
	bufChannel := make(chan *Table, 10)
	msgChannel := make(chan *Message, 20)
	CountCpu = runtime.NumCPU() - 1
	Conns = int(CountCpu/3)
	if Conns > 5 {
		Conns = 5
	}

	switch cfg.SaveAs {
	case "database":
		config.Logger.Info("Запуск соединения с базой данных...")
		for i := 0; i < Conns; i++ {
			egg.Go(func() error {
				return SaveDB(bufChannel)
			})
		}
	// case "database_json":
	// 	config.Logger.Info("Поток сохранения в БД и JSON стартовал!")
	// 	for i := 0; i < conns; i++ {
	// 		egg.Go(func() error {
	// 			return SaveDB(bufChannel)
	// 		})
	// 	}
	// 	for i := 0; i < conns; i++ {
	// 		egg.Go(func() error {
	// 			return SaveJson(cfg.SaveDir, bufChannel)
	// 		})
	// 	}
	// case "database_jsonSec":
	// 	config.Logger.Info("Поток сохранения в БД и JSON по секциям стартовал!")
	// 	for i := 0; i < conns; i++ {
	// 		egg.Go(func() error {
	// 			return SaveDB(bufChannel)
	// 		})
	// 	}
	// 	for i := 0; i < conns; i++ {
	// 		egg.Go(func() error {
	// 			return SaveMessage(cfg.SaveDir, msgChannel)
	// 		})
	// 	}
	case "clickhouse":
		config.Logger.Info("Поток сохранения в ClickHouse стартовал!")
		if err := ch.CheckTable(cfg); err != nil {
			return err
		}
		for i := 0; i < Conns; i++ {
			egg.Go(func() error {
				err := ExportBatch(bufChannel, cfg)
				if err != nil {
					config.Logger.WithError(err).Error("Ошибка clickhouse!")
					return err
				}
				return nil
			})
		}
	case "json":
		config.Logger.Info("Поток сохранения в JSON стартовал!")
		for i := 0; i < Conns+1; i++ {
			egg.Go(func() error {
				return SaveJson(cfg.SaveDir, bufChannel)
			})
		}
	case "jsonSec":
		config.Logger.Info("Поток сохранения в JSON по секциям стартовал!")
		for i := 0; i < Conns+1; i++ {
			egg.Go(func() error {
				return SaveMessage(cfg.SaveDir, msgChannel)
			})
		}

	default:
		config.Logger.Warn("Неизвестный тип сохранения!")
		close(bufChannel)
		close(msgChannel)
		os.Exit(1)
	}
	// err := Parse(dirPath, cfg, bufChannel, msgChannel)
	// if err != nil {
	// 	config.Logger.WithError(err).Error("Ошибка при обработке файлов!")
	// 	close(bufChannel)
	// 	close(msgChannel)
	// 	return err
	// }
	file, err:=strconv.Atoi(cfg.CountFilePerTick)
	if err!=nil{
		return err
	}
	if file<=0{
		return errors.New("Некорректно указана переменая COUNT_FILE_PER_TICK!")
	}
	for i := 0; i < file; i++ {
		eg.Go(func() error {
			config.Logger.Info("Парсер стартовал!")
			return Parse(dirPath, cfg, bufChannel, msgChannel)
		})
	}
	if err := eg.Wait(); err != nil {
		config.Logger.WithError(err).Error("Ошибка при обработке файлов!")
		close(bufChannel)
		close(msgChannel)
		return err
	}
	close(bufChannel)
	close(msgChannel)
	if err := egg.Wait(); err != nil {
		config.Logger.WithError(err).Error("Ошибка при сохранении файлов!")
		return err
	}
	return nil
}

func Parse(dirPath []fs.DirEntry, cfg *config.Config, bufChannel chan *Table, msg chan *Message) error {
	// Проверяет каждый элемент в папке и, если это файл отправляет его на чтение
	SaveAs = cfg.SaveAs
	for _, file := range dirPath {
		if !file.IsDir() {
			filePath := filepath.Join(cfg.SrcDir, file.Name())
			if filePath == "" {
				config.Logger.Warn("Пустой путь к файлу!")
				continue
			}
			if cfg.SaveAs == "database" {
				ok := checkExist(filePath)
				if !ok {
					continue
				}
			}
			if cfg.SaveAs == "clickhouse" {
				ok := checkExistCh(filePath, cfg)
				if !ok {
					continue
				}
			}
			gribFile, err := os.Open(filePath)
			if err != nil {
				config.Logger.WithField("file", filePath).WithError(err).Warn("Ошибка при открытии файла")
				return err
			}
			defer gribFile.Close()
			config.Logger.WithField("file",filePath).Info("Парсер стартовал...")
			if err:=readMessages(gribFile,bufChannel,msg); err!=nil{
				return err
			}
			// errGroup.Go(func() error {
			// 	config.Logger.WithField("file", filePath).Info("Парсер стартовал")
			// 	err := readMessages(gribFile, bufChannel, msg)
			// 	if err!=nil{
			// 		config.Logger.WithError(err).Error("Ошибка чтения файла!")
			// 	}
			// 	return nil
			// })

		}
	}

	// if err := errGroup.Wait(); err != nil {
	// 	config.Logger.WithError(err).Error("Ошибка при обработке файлов")
	// 	return err
	// }

	config.Logger.Info("Обработка файлов завершена")

	return nil
}
func checkExistCh(filePath string, cfg *config.Config) bool {
	gribFile, err := os.Open(filePath)
	if err != nil {
		config.Logger.WithField("file", filePath).WithError(err).Warn("Ошибка при открытии файла")
		return false
	}
	defer gribFile.Close()
	fileInfo, err := gribFile.Stat()
	if err != nil {
		return false
	}
	clickhouseConn, err := ch.GetConn(cfg)
	if err != nil {
		config.Logger.WithError(err).Error("Ошибка получения соединения!")
		return false
	}
	q_string := fmt.Sprintf("SELECT COUNT() FROM file_name WHERE file_name = '%s'", fileInfo.Name())
	var count uint64
	err = clickhouseConn.QueryRow(context.Background(), q_string).Scan(&count)
	if err != nil {
		config.Logger.WithError(err).Error("Ошибка выполнения запроса!")
		return false
	}
	if count != 0 {
		return false
	}
	err = clickhouseConn.Exec(context.Background(), "INSERT INTO file_name(file_name) VALUES($1)", fileInfo.Name())
	if err != nil {
		return false
	}
	return true
}
func checkExist(filePath string) bool {
	gribFile, err := os.Open(filePath)
	if err != nil {
		config.Logger.WithField("file", filePath).WithError(err).Warn("Ошибка при открытии файла")
		return false
	}
	defer gribFile.Close()

	conn, err := database.Dbpool.Acquire(context.Background())
	if err != nil {
		config.Logger.Warn("Ошибка при создании подключения: ", err)
		conn.Release()
		return false
	}
	//-------------------------
	hash := sha256.New()
	if _, err := io.Copy(hash, gribFile); err != nil {
		config.Logger.Warn(err)
		conn.Release()
		return false
	} else {

		hashInBytes := hash.Sum(nil)
		hashinstring := hex.EncodeToString(hashInBytes)
		var gribHash string
		err := conn.QueryRow(context.Background(), "SELECT grib_hash FROM hashes WHERE grib_hash=$1", hashinstring).Scan(&gribHash)
		if err != nil {
			if err == pgx.ErrNoRows {
				config.Logger.Info("Запись не найдена")
				_, err := conn.Exec(context.Background(), "INSERT INTO hashes (grib_hash) VALUES ($1)", hashinstring)
				if err != nil {
					config.Logger.Warn("Ошибка при выполнении запроса: ", err)
					conn.Release()
					return false
				}
				config.Logger.Info("Запись успешно добавлена! ")
				conn.Release()
				return true
			} else {
				config.Logger.Warn("Ошибка при выполнении запроса: ", err)
				conn.Release()
				return false
			}
		} else {
			config.Logger.Info("Хеш-запись найдена: ", gribHash)
			conn.Release()
			return false
		}

	}
	//-------------------------
}

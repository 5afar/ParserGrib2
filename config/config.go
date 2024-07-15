// Пакет парсит конфиг файл и сохраняет полученные строки в структуру
package config

import (
	"os"

	"github.com/sirupsen/logrus"
)

// Структура config-файла, в которой хранятся прочитанные строки
type Config struct {
	CHPort           string
	CHHost           string
	CHUser           string
	CHBase           string
	CHPass           string
	PGPort           string
	PGHost           string
	PGUser           string
	PGBase           string
	PGPass           string
	SaveDir          string
	MoveDir          string
	SrcDir           string
	SaveAs           string
	CountFilePerTick string
}

// Создание логера, записывающего данные в файл
var Logger *logrus.Logger

// getEnv Получает значения по ключу из переменной среды, которые загружены из .env файла
func getEnv(key string, defaultVal string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultVal
}

// LoggerStart устанавливает параметры для логера и готовит его к работе
func LoggerStart(file *os.File) {
	Logger = logrus.New()

	Logger.SetFormatter(&logrus.TextFormatter{})
	Logger.SetLevel(logrus.DebugLevel)
	Logger.SetOutput(file)
}

// New Заполняет структуру Config параметрами из .env файла
func New() *Config {
	return &Config{
		CHPort:           getEnv("CH_PORT", ""),
		CHHost:           getEnv("CH_HOST", ""),
		CHUser:           getEnv("CH_USER", ""),
		CHBase:           getEnv("CH_BASE", ""),
		CHPass:           getEnv("CH_PASS", ""),
		PGPort:           getEnv("PG_PORT", ""),
		PGHost:           getEnv("PG_HOST", ""),
		PGUser:           getEnv("PG_USER", ""),
		PGBase:           getEnv("PG_BASE", ""),
		PGPass:           getEnv("PG_PASS", ""),
		SaveDir:          getEnv("GRIB_SAVE_DIR", ""),
		MoveDir:          getEnv("MOVE_DIR", ""),
		SrcDir:           getEnv("SOURCE_DIR", ""),
		SaveAs:           getEnv("SAVE_AS", ""),
		CountFilePerTick: getEnv("COUNT_FILE_PER_TICK", ""),
	}
}

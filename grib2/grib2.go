// Парсинг GRIB2-фйалов
//
// Пакет читает файлы и отправляет полученные сообщения на запись
package grib2

import (
	"bytes"

	"encoding/binary"
	"errors"
	"fmt"
	"gribV2.com/config"
	"io"
	"log"


	"time"

	"github.com/google/uuid"

)

// Ряд констант предназначенных для разметки бинарного кода GRIB2
const (
	Grib                 = 0x47524942
	EndSectionLength     = 926365495
	SupportedGribEdition = 2
)

// readMessages Основная функция, которая запускает чтение мета-данных из начала файла, затем начинает читать секции и сообщения, после отправляет полученные данные на запись в опреедленном формате
func readMessages(file io.Reader, bufChannel chan<- *Table, msg chan<- *Message) error {
	defer config.Logger.Info("Чтение файла завершено")
	for {
		// Чтение мета-данных из начала файла
		err := readMeta(file)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		// Начинает читать сообщения из файла согласно разметке GRIB2 <GRIB ----- 7777>
		message, err := readMessage(file)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil /// Конец файла
			}
			config.Logger.WithError(err).Error("Ошибка чтения сообщения")
			return err
		}
		// Создание переменных, которые будут в последствии записаны
		// //-----------------------------------------------------------------------------------------------------
		// UUID
		id := uuid.New()
		// timestamp
		date := time.Date(int(message.Section1.ReferenceTime.Year), time.Month(message.Section1.ReferenceTime.Month), int(message.Section1.ReferenceTime.Day), int(message.Section1.ReferenceTime.Hour), int(message.Section1.ReferenceTime.Minute), int(message.Section1.ReferenceTime.Second), 0, time.UTC)
		// forecasttime (время прогноза)
		forcasttime := message.Section4.ProductDefinitionTemplate.ForecastTime
		// parameter (температура, давление, влажность...)
		param := ReadProductDisciplineCategoryParameters(uint16(message.Section0.Discipline), message.Section4.ProductDefinitionTemplate.ParameterCategory, message.Section4.ProductDefinitionTemplate.ParameterNumber)
		// surface_type Тип поверхности
		surfaceType := ReadSurfaceTypesUnits(int(message.Section4.ProductDefinitionTemplate.FirstSurface.Type))
		// surface_value Высота
		surfaceValue := fmt.Sprintf("%dm", message.Section4.ProductDefinitionTemplate.FirstSurface.Value)
		//Параметры сетки сохраняются в формате json
		var s3 S3
		// Название сетки
		s3.Name = Name
		// Ее параметры
		s3.Sec3 = message.Section3
		//
		// grib_data Массив точек float64
		data := message.Section7.Data
		// grib_data_int Массив точек int
		data_int := make([]int, len(message.Section7.Data))
		for i, v := range message.Section7.Data {
			data_int[i] = int(v)
		}
		// Структура, записываемая в базу данных
		table := &Table{
			UUID:         id,
			Date:         date,
			ForecastTime: forcasttime,
			Param:        param,
			SurfaceType:  surfaceType,
			SurfaceValue: surfaceValue,
			Section3:     s3,
			Data:         data,
			Data_int:     data_int,
		}
		// Если требуется сохранение в json по секциям, как в сообщении, то отправляется message, а не table
		if SaveAs == "jsonSec" {
			msg <- message
		} else {
			bufChannel <- table
		}
	}

}
// Параметры сетки
type S3 struct {
	Name string `json:"name"`
	Sec3 Section3
}

// readMeta Читает мета-данные из начала файла и отсеивает их
func readMeta(file io.Reader) error {
	var b byte
	for {
		err := binary.Read(file, binary.BigEndian, &b)
		if err != nil {
			return err
		}
		if b == 'G' {
			err := binary.Read(file, binary.BigEndian, &b)
			if err != nil {
				return err
			}
			if b == 'R' {
				err := binary.Read(file, binary.BigEndian, &b)
				if err != nil {
					return err
				}
				if b == 'I' {
					err := binary.Read(file, binary.BigEndian, &b)
					if err != nil {
						return err
					}
					if b == 'B' {
						return nil
					}
				}
			}
		}
	}
}

// readMessage Читает одно сообщение и отдает его на запись
func readMessage(file io.Reader) (*Message, error) {
	//Создает новую структуру Message
	message := Message{}
	// Читает Секцию 0
	sec0, headErr := readSec0(file)
	if headErr != nil {
		return &message, headErr
	}

	// Создание массива байт размером полученным из Секции 0
	msgBytes := make([]byte, sec0.MessageLength-16)
	// Читает все оставшиеся байты в этот массив
	numBytes, readErr := file.Read(msgBytes)

	if readErr != nil {
		config.Logger.Warn("Ошибка чтения сообщения!")
		return &message, readErr
	}
	if numBytes != int(sec0.MessageLength-16) {
		config.Logger.Warn("Все сообщение не прочитано!")
		errString := fmt.Sprintf("Ожидаемый размер(%v) и прочитанный(%v) не совпадают!", int(sec0.MessageLength-16), numBytes)
		return &message, errors.New(errString)
	}
	// Возвращает результат работы функции readMsg, которая парсит следущие секции
	return readMsg(bytes.NewReader(msgBytes), sec0)
}

// readSec0 Парсит Секцию 0 согласно ее структуре
func readSec0(file io.Reader) (sec0 Section0, err error) {

	err = binary.Read(file, binary.BigEndian, &sec0)
	if err != nil {
		return sec0, err
	}
	if Grib == Grib {
		if sec0.Edition != SupportedGribEdition {
			return sec0, errors.New("Ошибка формата гриба")
		}
	} else {
		return sec0, errors.New("Ошибка индикатора")
	}
	return
}

// readMsg читает оставшиеся секции из сообщения
func readMsg(msg io.Reader, sec0 Section0) (*Message, error) {
	message := Message{
		Section0: sec0,
	}
	for {
		// Читает заголовок секции, чтобы понять какую секцию читать
		sectionHead, headErr := readSectionHead(msg)
		if headErr != nil {
			log.Println("Error reading header", headErr.Error())
			return &message, headErr
		}
		// Проверка длинные заголовка
		if sectionHead.ContentLength() > 0 {
			var rawData = make([]byte, sectionHead.ContentLength())
			err := binary.Read(msg, binary.BigEndian, &rawData)
			if err != nil {
				return &message, err
			}
			byteReader := bytes.NewBuffer(rawData)
			// Выбор секции
			switch sectionHead.Number {

			case 1:
				message.Section1, err = ReadSection1(byteReader, sectionHead.ContentLength())
			case 2:
				message.Section2, err = ReadSection2(byteReader, sectionHead.ContentLength())
			case 3:
				message.Section3, err = ReadSection3(byteReader, sectionHead.ContentLength())
			case 4:
				message.Section4, err = ReadSection4(byteReader, sectionHead.ContentLength())
			case 5:
				message.Section5, err = ReadSection5(byteReader, sectionHead.ContentLength())
			case 6:
				message.Section6, err = ReadSection6(byteReader, sectionHead.ContentLength())
			case 7:
				message.Section7, err = ReadSection7(byteReader, sectionHead.ContentLength(), message.Section5)
			case 8:
				// end-section, return
				return &message, nil

			default:
				err = fmt.Errorf("Unknown section number %d  (Something bad with parser or files)", sectionHead.Number)
			}
			if err != nil {
				return &message, err
			}
		} else {
			return &message, nil
		}
	}
}
// readSectionHead Читает заголовок секции, определяет его номер и размер
func readSectionHead(msg io.Reader) (head SectionHead, err error) {
	var length uint32
	err = binary.Read(msg, binary.BigEndian, &length)
	if err != nil {
		return head, err
	}
	if length == EndSectionLength {
		return SectionHead{
			ByteLength: 4,
			Number:     8,
		}, nil
	}
	var sectionNumber uint8
	err = binary.Read(msg, binary.BigEndian, &sectionNumber)
	if err != nil {
		return head, err
	}

	return SectionHead{
		ByteLength: length,
		Number:     sectionNumber,
	}, nil
}

///-----------------------------------------------------------------------------------------------

// Message Сообщение включает в себя секции
type Message struct {
	Section0 Section0
	Section1 Section1
	Section2 Section2
	Section3 Section3
	Section4 Section4
	Section5 Section5
	Section6 Section6
	Section7 Section7
}

// | Octet Number | Content
// -----------------------------------------------------------
// | 1-4          | Length of the section in octets (21 or N)
// | 5            | Number of the section (1)
type SectionHead struct {
	ByteLength uint32 `json:"byteLength"`
	Number     uint8  `json:"number"`
}

func (s SectionHead) ContentLength() int {
	return int(s.ByteLength) - binary.Size(s)
}

// | Octet Number | Content
// -----------------------------------------------------------------------------------------
// | 1-4          | 'GRIB' (Coded according to the International Alphabet Number 5)
// | 5-6          | reserved
// | 7            | Discipline (From Table 0.0)
// | 8            | Edition number - 2 for GRIB2
// | 9-16         | Total length of GRIB message in octets (All sections);
type Section0 struct {
	// Indicator  uint32 `json:"indicator"`
	Reserved      uint16 `json:"reserved"`
	Discipline    uint8  `json:"discipline"`
	Edition       uint8  `json:"edition"`
	MessageLength uint64 `json:"messageLength"`
}


// | Octet Number | Content
// -----------------------------------------------------------------------------------------
// | 1-4          | Length of the section in octets (21 or N)
// | 5            | Number of the section (1)
// | 6-7          | Identification of originating/generating center (See Table 0) (See note 4)
// | 8-9          | Identification of originating/generating subcenter (See Table C)
// | 10           | GRIB master tables version number (currently 2) (See Table 1.0) (See note 1)
// | 11           | Version number of GRIB local tables used to augment Master Tables (see Table 1.1)
// | 12           | Significance of reference time (See Table 1.2)
// | 13-14        | Year (4 digits)
// | 15           | Month
// | 16           | Day
// | 17           | Hour
// | 18           | Minute
// | 19           | Second
// | 20           | Production Status of Processed data in the GRIB message (See Table 1.3)
// | 21           | Type of processed data in this GRIB message (See Table 1.4)
// | 22-N         | Reserved
type Section1 struct {
	OriginatingCenter         uint16 `json:"ooriginatingCenter"`
	OriginatingSubCenter      uint16 `json:"originatingSubCenter"`
	MasterTablesVersion       uint8  `json:"masterTablesVersion"`
	LocalTablesVersion        uint8  `json:"localTablesVersion"`
	ReferenceTimeSignificance uint8  `json:"referenceTimeSignificance"` // Table 1.2, value 1 is start of forecast
	ReferenceTime             Time   `json:"referenceTime"`
	ProductionStatus          uint8  `json:"productionStatus"`
	Type                      uint8  `json:"type"` // data type, Table 1.4, value 1 is forecast products
}

// ReadSection1 Читает определенный в заголовке размер байт в структуру Section1
func ReadSection1(f io.Reader, length int) (section Section1, err error) {
	return section, binary.Read(f, binary.BigEndian, &section)
}

// | Octet Number | Content
// ---------------------------------
// | 13-14        | Year (4 digits)
// | 15           | Month
// | 16           | Day
// | 17           | Hour
// | 18           | Minute
// | 19           | Second
type Time struct {
	Year   uint16 `json:"year"`   // year
	Month  uint8  `json:"month"`  // month + 1
	Day    uint8  `json:"day"`    // day
	Hour   uint8  `json:"hour"`   // hour
	Minute uint8  `json:"minute"` // minute
	Second uint8  `json:"second"` // second
}

// | Octet Number | Content
// -----------------------------------------------------------------------------------------
// | 1-4          | Length of the section in octets (N)
// | 5            | Number of the section (2)
// | 6-N          | Local Use
type Section2 struct {
	LocalUse []uint8 `json:"localUse"`
}
// ReadSection2 Читает определенный в заголовке размер байт в структуру Section2
func ReadSection2(f io.Reader, len int) (section Section2, err error) {
	section.LocalUse = make([]uint8, len)
	return section, read(f, &section.LocalUse)
}
// read Читает из массива байт нужную длинну
func read(reader io.Reader, data ...interface{}) (err error) {
	for _, what := range data {
		err = binary.Read(reader, binary.BigEndian, what)
		if err != nil {
			return err
		}
	}
	return nil
}

// | Octet Number | Content
// -----------------------------------------------------------------------------------------
// | 1-4          | Length of the section in octets (nn)
// | 5            | Number of the section (3)
// | 6            | Source of grid definition (See Table 3.0) (See note 1 below)
// | 7-10         | Number of data points
// | 11           | Number of octets for optional list of numbers defining number of points (See note 2 below)
// | 12           | Interpetation of list of numbers defining number of points (See Table 3.11)
// | 13-14        | Grid definition template number (= N) (See Table 3.1)
// | 15-xx        | Grid definition template (See Template 3.N, where N is the grid definition template
// |              | number given in octets 13-14)
// | [xx+1]-nn    | Optional list of numbers defining number of points (See notes 2, 3, and 4 below)
type Section3 struct {
	Source                   uint8       `json:"source"`
	DataPointCount           uint32      `json:"datapointCount"`
	PointCountOctets         uint8       `json:"pointCountOctets"`
	PointCountInterpretation uint8       `json:"pointCountInterpretation"`
	TemplateNumber           uint16      `json:"templateNumber"`
	Definition               interface{} `json:"definition"`
}
// ReadSection3 Читает определенный в заголовке размер байт в структуру Section3
func ReadSection3(f io.Reader, _ int) (section Section3, err error) {
	err = read(f, &section.Source, &section.DataPointCount, &section.PointCountOctets, &section.PointCountInterpretation, &section.TemplateNumber)
	if err != nil {
		return section, err
	}
	// Определяет сетку и записывает ее в структуру
	section.Definition, err = ReadGrid(f, section.TemplateNumber)
	return section, err
}

// | Octet Number | Content
// -----------------------------------------------------------------------------------------
// | 1-4          | Length of the section in octets (nn)
// | 5            | Number of the section (4)
// | 6-7          | Number of coordinate values after template (See note 1 below)
// | 8-9          | Product definition template number (See Table 4.0)
// | 10-xx        | Product definition template (See product template 4.X, where X is
// |              | the number given in octets 8-9)
// | [xx+1]-nn    | Optional list of coordinate values (See notes 2 and 3 below)
type Section4 struct {
	CoordinatesCount                uint16   `json:"coordinatesCount"`
	ProductDefinitionTemplateNumber uint16   `json:"productDefinitionTemplateNumber"`
	ProductDefinitionTemplate       Product0 `json:"productDefinitionTemplate"` // FIXME
	Coordinates                     []byte   `json:"coordinates"`
}
// ReadSection4 Читает определенный в заголовке размер байт в структуру Section4
func ReadSection4(f io.Reader, length int) (section Section4, err error) {
	err = read(f, &section.CoordinatesCount, &section.ProductDefinitionTemplateNumber)
	if err != nil {
		return section, err
	}
	switch section.ProductDefinitionTemplateNumber {
	case 0:
		err = read(f, &section.ProductDefinitionTemplate)
	default:
		return section, nil
	}
	if err != nil {
		return section, err
	}
	section.Coordinates = make([]byte, section.CoordinatesCount)
	return section, read(f, &section.Coordinates)
}

// | Octet Number | Content
// -----------------------------------------------------------------------------------------
// | 1-4          | Length of the section in octets (nn)
// | 5            | Number of the section (5)
// | 6-9          | Number of data points where one or more values are specified in Section 7 when a bit map is present,
// |              | total number of data points when a bit map is absent.
// | 10-11        | Data representation template number (See Table 5.0 http://www.nco.ncep.noaa.gov/pmb/docs/grib2/grib2_doc/grib2_table5-0.shtml)
// | 12-nn        | Data representation template (See Template 5.X, where X is the number given in octets 10-11)
type Section5 struct {
	PointsNumber       uint32 `json:"pointsNumber"`
	DataTemplateNumber uint16 `json:"dataTemplateNumber"`
	//DataTemplate       Data3  `json:"dataTemplate"` // FIXME
	Data []byte `json:"dataTemplate"`
}
// ReadSection5 Читает определенный в заголовке размер байт в структуру Section5
func ReadSection5(f io.Reader, length int) (section Section5, err error) {
	section.Data = make([]byte, length-6)
	err = read(f, &section.PointsNumber, &section.DataTemplateNumber, &section.Data)
	if err != nil {
		return section, err
	}
	if section.DataTemplateNumber != 0 && section.DataTemplateNumber != 2 && section.DataTemplateNumber != 3 {
		return section, fmt.Errorf("Template number not supported: %d", section.DataTemplateNumber)
	}
	return section, nil
}
// GetDataTemplate Получает шаблон хранения данных согласно структуре секций
func (section Section5) GetDataTemplate() (interface{}, error) {
	switch section.DataTemplateNumber {

	case 0:
		data := Data0{}
		read(bytes.NewReader(section.Data), &data)
		return data, nil
	case 2:
		data := Data2{}
		read(bytes.NewReader(section.Data), &data)
		return data, nil
	case 3:
		data := Data3{}
		read(bytes.NewReader(section.Data), &data)
		return data, nil
	}
	return struct{}{}, fmt.Errorf("Unknown data format")
}

//	| Octet Number | Content
//	-----------------------------------------------------------------------------------------
//	| 1-4          | Length of the section in octets (nn)
//	| 5            | Number of the section (6)
//	| 6            | Bit-map indicator (See Table 6.0) (See note 1 below)
//	| 7-nn         | Bit-map
//
// If octet 6 is not zero, the length of this section is 6 and octets 7-nn are not present.
type Section6 struct {
	BitmapIndicator uint8  `json:"bitmapIndicator"`
	Bitmap          []byte `json:"bitmap"`
}
// ReadSection6 Читает определенный в заголовке размер байт в структуру Section6
func ReadSection6(f io.Reader, length int) (section Section6, err error) {
	section.Bitmap = make([]byte, length-1)
	return section, read(f, &section.BitmapIndicator, &section.Bitmap)
}

// | Octet Number | Content
// -----------------------------------------------------------------------------------------
// | 1-4          | Length of the section in octets (nn)
// | 5            | Number of the section (7)
// | 6-nn         | Data in a format described by data Template 7.X, where X is the data representation template number
// |              | given in octets 10-11 of Section 5.
type Section7 struct {
	Data []float64 `json:"data"`
}
// ReadSection7 Читает определенный в заголовке размер байт в структуру Section7
func ReadSection7(f io.Reader, length int, section5 Section5) (section Section7, sectionError error) {
	defer func() {
		if err := recover(); err != nil {
			log.Printf("Corrupt message %q\n", err)
		}
	}()
	data, sectionError := section5.GetDataTemplate()
	if sectionError != nil {
		return Section7{}, sectionError
	}
	if length != 0 {
		switch x := data.(type) {
		case Data0:
			section.Data, sectionError = ParseData0(f, length, &x)
		case Data2:
			section.Data, sectionError = ParseData2(f, length, &x)
		case Data3:
			section.Data, sectionError = ParseData3(f, length, &x)
		default:
			sectionError = fmt.Errorf("Unknown data type")
			return
		}

	}
	return section, sectionError
}

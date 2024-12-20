package main

import (
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	_ "github.com/lib/pq"
	"github.com/schollz/progressbar/v3"
	"gopkg.in/yaml.v2"
)

type Config struct {
	DBUser     string   `yaml:"db_user"`
	DBPassword string   `yaml:"db_password"`
	DBHost     string   `yaml:"db_host"`
	DBPort     int      `yaml:"db_port"`
	DBName     string   `yaml:"db_name"`
	TableName  string   `yaml:"table_name"`
	Columns    []string `yaml:"columns"`
	ChunkSize  int      `yaml:"chunk_size"`
	OutputDir  string   `yaml:"output_dir"`
}

func writeChunkToCSV(records [][]string, chunkNum int, headers []string, tableName, outputDir string) error {
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return err
	}

	filename := filepath.Join(outputDir, fmt.Sprintf("%s_chunk_%d.csv", tableName, chunkNum))
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	if err := writer.Write(headers); err != nil {
		return err
	}

	return writer.WriteAll(records)
}

func main() {
	configPath := flag.String("config", "config.yaml", "Path to the config file")
	flag.Parse()

	configFile, err := os.ReadFile(*configPath)
	if err != nil {
		panic("Error reading config file")
	}

	var config Config
	err = yaml.Unmarshal(configFile, &config)
	if err != nil {
		fmt.Println(err)
		panic("Error parsing config file")
	}

	db, err := sql.Open("postgres", fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable", config.DBUser, config.DBPassword, config.DBHost, config.DBPort, config.DBName))
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Get total count
	var totalCount int
	err = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", config.TableName)).Scan(&totalCount)
	if err != nil {
		panic(err)
	}

	headers := config.Columns
	quotedHeaders := make([]string, len(headers))
	for i, header := range headers {
		quotedHeaders[i] = fmt.Sprintf(`"%s"`, header)
	}
	query := fmt.Sprintf("SELECT %s FROM %s OFFSET $1 LIMIT $2", strings.Join(quotedHeaders, ","), config.TableName)

	bar := progressbar.Default(int64(totalCount))
	offset := 0
	chunkNum := 1

	for offset < totalCount {
		rows, err := db.Query(query, offset, config.ChunkSize)
		if err != nil {
			panic(err)
		}

		var chunk [][]string
		for rows.Next() {
			record := make([]interface{}, len(headers))
			for i := range record {
				record[i] = new(interface{})
			}

			if err := rows.Scan(record...); err != nil {
				rows.Close()
				panic(err)
			}

			row := make([]string, len(record))
			for i, v := range record {
				val := v.(*interface{})
				if *val == nil {
					row[i] = ""
				} else {
					row[i] = fmt.Sprintf("%v", *val)
				}
			}
			chunk = append(chunk, row)
			bar.Add(1)
		}
		rows.Close()

		if err := writeChunkToCSV(chunk, chunkNum, headers, config.TableName, config.OutputDir); err != nil {
			panic(err)
		}

		offset += config.ChunkSize
		chunkNum++
	}

	fmt.Printf("\nExported %d records to %s directory\n", totalCount, config.OutputDir)
}

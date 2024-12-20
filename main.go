package main

import (
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	_ "github.com/lib/pq"
	"github.com/vbauerster/mpb"
	"github.com/vbauerster/mpb/decor"
	"gopkg.in/yaml.v2"
)

type TableConfig struct {
	Name      string   `yaml:"name"`
	Columns   []string `yaml:"columns"`
	OutputDir string   `yaml:"output_dir"`
}

type Config struct {
	DBUser       string        `yaml:"db_user"`
	DBPassword   string        `yaml:"db_password"`
	DBHost       string        `yaml:"db_host"`
	DBPort       int           `yaml:"db_port"`
	DBName       string        `yaml:"db_name"`
	Tables       []TableConfig `yaml:"tables"`
	ChunkSize    int           `yaml:"chunk_size"`
	Concurrently bool          `yaml:"concurrently"`
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

func processTable(db *sql.DB, config Config, table TableConfig, wg *sync.WaitGroup, p *mpb.Progress) {
	defer wg.Done()

	var headers []string
	if len(table.Columns) == 0 {
		// Get all columns if not specified
		rows, err := db.Query(fmt.Sprintf("SELECT column_name FROM information_schema.columns WHERE table_name='%s'", table.Name))
		if err != nil {
			panic(err)
		}
		defer rows.Close()

		for rows.Next() {
			var columnName string
			if err := rows.Scan(&columnName); err != nil {
				panic(err)
			}
			headers = append(headers, columnName)
		}
	} else {
		headers = table.Columns
	}

	quotedHeaders := make([]string, len(headers))
	for i, header := range headers {
		quotedHeaders[i] = fmt.Sprintf(`"%s"`, header)
	}
	query := fmt.Sprintf("SELECT %s FROM %s OFFSET $1 LIMIT $2", strings.Join(quotedHeaders, ","), table.Name)

	// Get total count
	var totalCount int
	err := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table.Name)).Scan(&totalCount)
	if err != nil {
		panic(err)
	}

	bar := p.AddBar(int64(totalCount),
		mpb.PrependDecorators(
			decor.Name(fmt.Sprintf("Processing %s: ", table.Name)),
			decor.CountersNoUnit("%d / %d"),
		),
		mpb.AppendDecorators(decor.Percentage()),
	)

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
			bar.Increment()
		}
		rows.Close()

		if err := writeChunkToCSV(chunk, chunkNum, headers, table.Name, table.OutputDir); err != nil {
			panic(err)
		}

		offset += config.ChunkSize
		chunkNum++
	}

	fmt.Printf("\nExported %d records from %s to %s directory\n", totalCount, table.Name, table.OutputDir)
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

	p := mpb.New()

	if config.Concurrently {
		var wg sync.WaitGroup
		p = mpb.New(mpb.WithWaitGroup(&wg))

		for _, table := range config.Tables {
			wg.Add(1)
			go processTable(db, config, table, &wg, p)
		}
		wg.Wait()
	} else {
		for _, table := range config.Tables {
			processTable(db, config, table, nil, p)
		}
	}

	p.Wait()
}

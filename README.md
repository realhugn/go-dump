# Go Dump Tool

## Overview

The Go Dump Tool is a utility designed to facilitate the extraction and dumping of data from a database into csv files. 

## Supported Databases

Currently, the Go Dump Tool supports dumping data from PostgreSQL databases to CSV files.
## Features

- Efficient data extraction
- Chunking to reduce memory usage
- Customizable configuration
- Supports multiple database types
- Easy to use

## Installation

To install the Go Dump Tool, follow these steps:

1. Clone the repository:
    ```sh
    git clone https://github.com/nothung209/go-dump.git
    ```
2. Navigate to the project directory:
    ```sh
    cd go-dump
    ```
3. Build the tool:
    ```sh
    go build
    ```

## Usage

To use the Go Dump Tool, run the following command:
```sh
./go-dump --config /path/to/config.yaml
```

Replace `/path/to/config.yaml` with the path to your configuration file.

## Customization

Customize the tool by setting the following parameters in a `config.yaml` file:

```yaml
chunk_size: 100000
db_user: changme
db_password: changme
db_host: changme
db_port: changme
db_name: changme
table_name: my_table
columns:
    - id
    - column1
    - column2
    - culumn3
output_dir: dumps
```

Adjust these values according to your database configuration and requirements.
package main

import (
	"fmt"
	"os"
	"bufio"
	"log"
	"strings"
)

// index structure: key -> [length (of the data to be read), offset (where is the key)]

func db_set(key string, data string, index map[string][2]int64) {
	file, err := os.OpenFile("database", os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
        log.Fatal(err)
	}
    defer file.Close()

	fileStats, err := os.Stat("database")
	if err != nil {
	    log.Fatal(err)
	}
	content := fmt.Sprintf("%s %s\n", key, data)
	contentLength := int64(len([]byte(content)))
	index[key] = [2]int64{fileStats.Size(), contentLength};
	file.WriteString(content)
	file.Close()
}

func db_getWithByteOffset(key string, index map[string][2]int64) string {
	offset := index[key][0]
	length := index[key][1]
	file, err := os.Open("database")
	if err != nil {
        log.Fatal(err)
	}
	buf := make([]byte, length)
	file.ReadAt(buf, offset)
	file.Close()
	return string(buf)
}

func db_get(key string) string {
	file, err := os.Open("database")
	if err != nil {
        log.Fatal(err)
	}
	scanner := bufio.NewScanner(file)

	data := ""
    for scanner.Scan() {
		line := scanner.Text()
		words := strings.Fields(line)
		if words[0] == key {
			data = words[1]
		}
    }

    if err := scanner.Err(); err != nil {
        log.Fatal(err)
	}
	
	return data
}

func main() {
	// index for keeping byte offset of keys
	index := make(map[string][2]int64)

	ary := index["4"]
	ary[0] = 176
	ary[1] = 9

	index["4"] = ary

	funcName := os.Args[1]
	keyName := os.Args[2]
	data := ""

	if funcName == "get" {
		fmt.Println(db_getWithByteOffset(keyName, index))
	} else {
		data = os.Args[3]
		db_set(keyName, data, index)
		fmt.Println(index)
		save_index(index)
		fmt.Println(fmt.Sprintf("Set %s and %s", keyName, data))
	}
	
}

func save_index(index map[string][2]int64) {
	file, err := os.OpenFile("index", os.O_WRONLY, 0644)
	if err != nil {
        log.Fatal(err)
	}
    defer file.Close()

	for k,v := range index {
		file.WriteString(fmt.Sprintf("%s %d\n", k, v))
	}
	file.Close()
}

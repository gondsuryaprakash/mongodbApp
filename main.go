package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	"github.com/jcelliott/lumber"
)

var version = "1.0.0"

type (
	Logger interface {
		Fatal(string, ...interface{})
		Error(string, ...interface{})
		Warn(string, ...interface{})
		Info(string, ...interface{})
		Debug(string, ...interface{})
		Trace(string, ...interface{})
	}

	Driver struct {
		mutex   sync.Mutex
		mutexes map[string]*sync.Mutex
		dir     string
		log     Logger
	}
)

type Option struct {
	Logger
}

func New(dir string, option *Option) (*Driver, error) {
	dir = filepath.Clean(dir)
	opts := Option{}

	if option != nil {
		opts = *option
	}

	if opts.Logger == nil {
		opts.Logger = lumber.NewConsoleLogger(lumber.INFO)
	}

	driver := Driver{
		dir:     dir,
		mutexes: make(map[string]*sync.Mutex),
		log:     opts.Logger,
	}
	fmt.Println(opts)
	if _, err := os.Stat(dir); err == nil {
		opts.Logger.Debug("Using '%s' (database Already Exist )\n", dir)
		return &driver, nil
	}

	opts.Logger.Debug("Creating new DataBase '%s'....\n", dir)

	return &driver, os.Mkdir(dir, 0755)

}

func (d *Driver) Write(collection, resourse string, v interface{}) error {
	if collection == "" {
		return fmt.Errorf("Collection is not present")
	}
	if resourse == "" {
		return fmt.Errorf("Resource is not present")
	}
	mutex := d.getOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()
	dir := filepath.Join(d.dir, collection)
	fnlPath := filepath.Join(dir, resourse+".json")
	tmpPath := fnlPath + ".tmp"
	if err := os.MkdirAll(dir, 0750); err != nil {
		return err
	}
	b, err := json.MarshalIndent(v, "", "\t")
	if err != nil {
		return err
	}
	b = append(b, byte('\n'))
	if err := ioutil.WriteFile(tmpPath, b, 0644); err != nil {
		return err
	}
	return os.Rename(tmpPath, fnlPath)
}

func (d *Driver) Read(collection, resource string, v interface{}) error {

	if collection == "" {
		return fmt.Errorf("Missing collection")
	}
	if resource == "" {
		return fmt.Errorf("Missing resource")
	}
	record := filepath.Join(d.dir, collection, resource)

	if _, err := stat(record); err != nil {
		return err
	}
	b, err := ioutil.ReadFile(record + ".json")
	if err != nil {
		return err
	}

	return json.Unmarshal(b, &v)
}

func (d *Driver) ReadAll(collection string) ([]string, error) {
	if collection == "" {
		return nil, fmt.Errorf("Missing Collection")
	}
	dir := filepath.Join(d.dir, collection)
	if _, err := stat(dir); err != nil {
		return nil, err
	}

	files, _ := ioutil.ReadDir(dir)

	var records []string

	for _, file := range files {
		b, err := ioutil.ReadFile(filepath.Join(dir, file.Name()))
		if err != nil {
			return nil, err
		}

		records = append(records, string(b))

	}

	return records, nil

}

func (d *Driver) Delete(collection, resource string) error {
	path := filepath.Join(collection, resource)

	mutex := d.getOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()
	dir := filepath.Join(d.dir, path)

	switch fi, err := stat(dir); {
	case fi == nil, err != nil:
		return fmt.Errorf("Unable to find file or directory")
	case fi.Mode().IsDir():
		return os.RemoveAll(dir)
	case fi.Mode().IsRegular():
		return os.RemoveAll(dir + ".json")
	}
	return nil

}

func (d *Driver) getOrCreateMutex(collection string) *sync.Mutex {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	m, ok := d.mutexes[collection]

	if !ok {
		m = &sync.Mutex{}
		d.mutexes[collection] = m
	}
	return m
}

func stat(path string) (fi os.FileInfo, err error) {
	if fi, err = os.Stat(path); os.IsNotExist(err) {
		fi, err = os.Stat(path + ".json")
	}
	return
}

type Address struct {
	City    string
	State   string
	Country string
	PinCode json.Number
}

type User struct {
	Name    string
	Age     json.Number
	Contact string
	Company string
	Address Address
}

func main() {
	dir := "./"
	db, err := New(dir, nil)
	if err != nil {
		fmt.Println("Err", err)
	}
	employee := []User{
		{"Surya", "23", "7068528089", "Josh Software", Address{"Bengaluru", "Karnataka", "India", "273303"}},
		{"Rahul", "23", "7068528089", "Google", Address{"Bengaluru", "Karnataka", "India", "273303"}},
		{"Sai", "23", "7068528089", "Microsoft", Address{"Bengaluru", "Karnataka", "India", "273303"}},
		{"Ritesh", "23", "7068528089", "FaceBook", Address{"Bengaluru", "Karnataka", "India", "273303"}},
		{"Nitesh", "23", "7068528089", "Remote", Address{"Bengaluru", "Karnataka", "India", "273303"}},
		{"Keerthi", "23", "7068528089", "Tracxn", Address{"Bengaluru", "Karnataka", "India", "273303"}},
		{"Nisha", "23", "7068528089", "Walmart", Address{"Bengaluru", "Karnataka", "India", "273303"}},
	}

	for _, element := range employee {
		db.Write("users", element.Name, User{
			Name:    element.Name,
			Age:     element.Age,
			Contact: element.Contact,
			Company: element.Company,
			Address: element.Address,
		})
	}

	records, err := db.ReadAll("users")

	if err != nil {
		fmt.Println("Err in Reading user's record", err)
	}

	allusers := []User{}

	for _, value := range records {
		employeeFound := User{}
		if err = json.Unmarshal([]byte(value), &employeeFound); err != nil {
			fmt.Println("Err in UnMarshal", err)
		}
		allusers = append(allusers, employeeFound)
	}

	fmt.Println(allusers)

	// // if err := db.Delete("users", "Surya"); err != nil {
	// // 	fmt.Println("Err in deleting Data", err)
	// // }

	// if err := db.Delete("users", ""); err != nil {
	// 	fmt.Println("Err in deleting data", err)
	// }
}

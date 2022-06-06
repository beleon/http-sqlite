package main

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"log"
	"mime/multipart"
	"net/http"
	"os"
	"path"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

var dbDir string = "dbs"
var dbs map[string]*sql.DB = map[string]*sql.DB{}
var authToken = ""

func main() {
	loadEnv()
	connectDbs()
	if authToken == "" {
		log.Println("no auth")
		//log.Panic("No auth token provided!")
	}

	http.HandleFunc("/", makeHandler())
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Panic(err)
	}
}

func makeHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || !strings.Contains(r.Header.Get("Content-type"), "multipart/form-data") {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		//dumpedBody, _ := httputil.DumpRequest(r, true)
		//fmt.Printf("%s\n", string(dumpedBody))

		r.ParseMultipartForm(512000000)

		token := r.PostFormValue("token")
		if token != authToken {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		dbId := r.PostFormValue("dbId")
		if dbId != "" {
			var db *sql.DB
			if dbtemp, ok := dbs[dbId]; ok {
				db = dbtemp
			} else {
				w.WriteHeader(http.StatusBadRequest)
				return
			}

			sqlQuery := r.PostFormValue("sqlQuery")
			args := []interface{}{}

			for _, h := range r.MultipartForm.File["sqlArg"] {
				file, err := h.Open()
				checkErr(err)
				defer file.Close()

				data, err := ioutil.ReadAll(file)
				checkErr(err)

				if err != nil {
					w.WriteHeader(http.StatusBadRequest)
					return
				}

				args = append(args, data)
			}

			if strings.HasPrefix(strings.ToLower(sqlQuery), "select ") {
				mw := multipart.NewWriter(w)
				defer mw.Close()
				w.Header().Set("Content-Type", mw.FormDataContentType())

				rows, err := db.Query(sqlQuery, args...)
				checkErr(err)
				var id int
				var s_type string
				var data []byte
				var timestamp string
				for rows.Next() {
					rows.Scan(&id, &s_type, &data, &timestamp)
					fw, err := mw.CreateFormField("id")
					checkErr(err)
					_, err = fw.Write([]byte(fmt.Sprintf("%d", id)))
					checkErr(err)
					fw, err = mw.CreateFormField("type")
					checkErr(err)
					_, err = fw.Write([]byte(s_type))
					checkErr(err)
					fw, err = mw.CreateFormField("data")
					checkErr(err)
					_, err = fw.Write(data)
					checkErr(err)
					fw, err = mw.CreateFormField("timestamp")
					checkErr(err)
					_, err = fw.Write([]byte(timestamp))
					checkErr(err)
				}
				return
			} else {
				tx, err := db.Begin()
				checkErr(err)
				res, err := tx.Exec(sqlQuery, args...)
				if err != nil {
					tx.Rollback()
				}
				checkErr(err)
				tx.Commit()
				id, err := res.LastInsertId()
				checkErr(err)
				fmt.Fprintf(w, "%d", id)
				return
			}
		} else {
			dbId = genId()
			path := path.Join(dbDir, fmt.Sprintf("%s.db", dbId))
			db, err := sql.Open("sqlite3", path)
			checkErr(err)
			tx, err := db.Begin()
			checkErr(err)
			_, err = tx.Exec("CREATE TABLE \"store\" (\"id\" INTEGER NOT NULL, \"type\" TEXT, \"data\" BLOB, \"timestamp\" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY(\"id\"))")
			if err != nil {
				tx.Rollback()
			}
			checkErr(err)
			tx.Commit()
			dbs[dbId] = db
			fmt.Fprint(w, dbId)
			return
		}
	}
}

func connectDbs() {
	fileinfos, err := ioutil.ReadDir(dbDir)
	checkErr(err)

	for _, fileinfo := range fileinfos {
		filename := fileinfo.Name()
		if !strings.HasSuffix(filename, ".db") {
			continue
		}
		path := path.Join(dbDir, filename)
		dbs[filename[:len(filename)-3]], err = sql.Open("sqlite3", path)
		checkErr(err)
	}
}

func loadEnv() {
	val := os.Getenv("TEXTS_DB_DIR")
	if val != "" {
		dbDir = val
	}
	val = os.Getenv("TEXTS_AUTH_TOKEN")
	if val != "" {
		authToken = val
	}
}

func checkErr(err error) {
	if err != nil {
		log.Panic(err)
	}
}

func genId() string {
	idBytes := make([]byte, 32)
	_, err := rand.Read(idBytes)
	checkErr(err)

	return hex.EncodeToString(idBytes)
}

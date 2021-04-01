package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func main() {

	// INIT SQL CONNECTION

	dsn := "root:root@tcp(127.0.0.1:3333)/?interpolateParams=true&parseTime=true"

	// uncomment to add custom certs

	// caCert := ""
	// serverCert := ""
	// serverKey := ""

	// rootCertPool := x509.NewCertPool()
	// if ok := rootCertPool.AppendCertsFromPEM([]byte(caCert)); !ok {
	// 	log.Fatalf("Failed to append PEM.")
	// 	return
	// }
	// clientCert := make([]tls.Certificate, 0, 1)
	// // certs, err := tls.LoadX509KeyPair("./server-cert.pem", "./server-key.pem")
	// certs, err := tls.X509KeyPair([]byte(serverCert), []byte(serverKey))
	// if err != nil {
	// 	log.Fatal(err)
	// 	return
	// }

	// clientCert = append(clientCert, certs)

	// mysql.RegisterTLSConfig("custom", &tls.Config{
	// 	RootCAs:      rootCertPool,
	// 	Certificates: clientCert,
	// })

	singleStore, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("init memsql connection err: %v", err)
		return
	}

	pingStart := time.Now()
	if err = singleStore.Ping(); err != nil {
		log.Fatalf("ping memsql connection err: %v", err)
		return
	}

	log.Printf("ping DB took %v", time.Now().Sub(pingStart))

	pingStart = time.Now()
	if err = singleStore.Ping(); err != nil {
		log.Fatalf("ping memsql connection err: %v", err)
		return
	}

	log.Printf("second ping DB took %v", time.Now().Sub(pingStart))

	singleStore.SetConnMaxLifetime(time.Second * 40)
	singleStore.SetMaxOpenConns(500)
	singleStore.SetMaxIdleConns(100)

	// limit execution to 15secs
	bgCtx := context.Background()
	ctx, _ := context.WithDeadline(bgCtx, time.Now().Add(15*time.Second))

	// PREPARE TEST DB

	// opens a new connection to the DB
	conn, err := singleStore.Conn(ctx)

	if err != nil {
		log.Fatalf("get sql connection error: %v", err)
		return
	}

	// create DB if doesnt exist
	dbName := "test_db"

	if _, err := conn.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %v;", dbName)); err != nil {
		log.Fatalf("create project DB error: %v", err)
		return
	}

	// use the DB
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("USE %v", dbName)); err != nil {
		log.Fatalf("use DB error: %v", err)
		return
	}

	if _, err = conn.ExecContext(ctx, `DROP TABLE IF EXISTS test;`); err != nil {
		log.Fatalf("drop test table error: %v", err)
	}

	if _, err = conn.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS test (
		id VARCHAR(60) NOT NULL,
		PRIMARY KEY (id),
		SHARD KEY (id)
	  );`); err != nil {
		log.Fatalf("create test table error: %v", err)
		return
	}

	// INSERT 1000 ROWS FOR TESTING

	log.Println("inserting rows...")

	values := []string{}
	for y := 0; y < 1000; y++ {
		values = append(values, fmt.Sprintf("(%v)", y))
	}
	if _, err = conn.ExecContext(ctx, fmt.Sprintf(`INSERT INTO test values %v;`, strings.Join(values, ","))); err != nil {
		log.Fatalf("insert test table error: %v", err)
		return
	}

	log.Println("done inserting rows")

	// LAUNCH 100 CONCURRENT WORKERS

	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {

		wg.Add(1)

		go func() {
			defer wg.Done()

			// opens a new connection to the DB
			conn, err := singleStore.Conn(ctx)

			if err != nil {
				log.Printf("get sql connection error: %v", err)
				return
			}
			defer conn.Close()

			// use the DB
			if _, err := conn.ExecContext(ctx, fmt.Sprintf("USE %v", dbName)); err != nil {
				log.Printf("set project DB error: %v", err)
				return
			}

			// start transaction
			tx, err := conn.BeginTx(ctx, nil)

			if err != nil {
				log.Printf("begin sql transaction error: %v", err)
				return
			}
			defer tx.Rollback()

			// get an atomic id
			var id string

			row := tx.QueryRowContext(ctx, "SELECT id FROM test LIMIT 1 FOR UPDATE")

			if err := row.Scan(&id); err != nil {
				if err == sql.ErrNoRows {
					log.Println("no more rows")
					return
				}

				log.Printf("scan id error: %v", err)
				return
			}

			time.Sleep(1 * time.Second)

			if _, err := tx.ExecContext(ctx, "DELETE FROM test WHERE id = ?", id); err != nil {
				log.Printf("delete row error: %v", err)
				return
			}

			if err := tx.Commit(); err != nil {
				log.Printf("commit sql transaction error: %v", err)
				return
			}

			return
		}()
	}

	wg.Wait()
	log.Println("finished, exiting")
}

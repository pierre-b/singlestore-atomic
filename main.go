package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"sync/atomic"

	"github.com/eapache/go-resiliency/semaphore"
	"github.com/go-sql-driver/mysql"
)

func main() {
	// add line numbers to log messages
	log.SetFlags(log.LstdFlags | log.Lshortfile)

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

	// limit execution to 10secs

	secs := 10
	multiplier := time.Duration(secs)
	duration := time.Second

	bgCtx := context.Background()
	ctx, _ := context.WithDeadline(bgCtx, time.Now().Add(multiplier*duration))

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
		finished BOOL DEFAULT FALSE NOT NULL,
		created TIMESTAMP DEFAULT NOW() NOT NULL,
		PRIMARY KEY (id),
		SHARD KEY (id)
	  );`); err != nil {
		log.Fatalf("create test table error: %v", err)
		return
	}

	if _, err = conn.ExecContext(ctx, `DROP TABLE IF EXISTS executions;`); err != nil {
		log.Fatalf("drop test table error: %v", err)
	}

	if _, err = conn.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS executions (
		id int AUTO_INCREMENT NOT NULL,
		task_id VARCHAR(60) NOT NULL,
		attempt int NOT NULL,
		PRIMARY KEY (id, task_id),
		SHARD KEY (task_id),
		UNIQUE KEY (task_id, attempt)
	  );`); err != nil {
		log.Fatalf("create test table error: %v", err)
		return
	}

	// INSERT ROWS FOR TESTING

	log.Println("inserting rows...")

	values := []string{}
	totalRows := 100000

	for y := 0; y < totalRows; y++ {
		values = append(values, fmt.Sprintf("(%v)", y))
	}
	if _, err = conn.ExecContext(ctx, fmt.Sprintf(`INSERT INTO test (id) values %v;`, strings.Join(values, ","))); err != nil {
		log.Fatalf("insert test table error: %v", err)
		return
	}

	log.Println("done inserting rows")

	// max tickets in parallel
	concurrency := 200
	sem := semaphore.New(concurrency, 6*time.Second)

	var maxTimeTaken time.Duration
	var wg sync.WaitGroup

	workStart := time.Now()
	shouldContinue := true
	var duplicatesCount uint64

	for shouldContinue == true {

		if err := sem.Acquire(); err != nil {
			continue
		}

		// tickets can wait in the Acquire() before the shouldContinue changed to false
		// check if we can still continue
		if shouldContinue == false {
			log.Println("abort, shouldContinue == false")
			continue
		}

		wg.Add(1)

		go func() {
			defer wg.Done()
			defer sem.Release()

			startTimer := time.Now()

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

			// the eventual dequeued test id
			var id string

			// infinite retry to dequeue a task
			retryCount := 0
			for {
				result, err := conn.ExecContext(ctx, `
					INSERT INTO executions (task_id, attempt)
					SELECT test.id, IFNULL(MAX(attempt) + 1, 1)
					FROM test
					LEFT JOIN executions ON test.id = executions.task_id
					WHERE test.finished = FALSE
					GROUP BY test.id
					HAVING MAX(executions.task_id) IS NULL
					ORDER BY RAND() ASC
					LIMIT 1
				`)
				if err != nil {
					mysqlErr, ok := err.(*mysql.MySQLError)
					if !ok {
						log.Printf("dequeue error: %v", err)
						return
					}

					// 1062 = ER_DUP_ENTRY
					// 1213 = ER_LOCK_DEADLOCK
					if mysqlErr.Number == 1062 || mysqlErr.Number == 1213 {
						atomic.AddUint64(&duplicatesCount, 1)
						// dequeue collision with another worker
						retryCount++
						continue
					}

					log.Printf("dequeue error: %v", err)
					return
				}

				// XXX: HANDLE no row dequeued
				executionID, err := result.LastInsertId()
				if err != nil {
					log.Printf("LastInsertId error: %v", err)
					return
				}

				// we have succesfully dequeued a test row
				// now we need to retrieve it's id
				row := conn.QueryRowContext(ctx, `
					SELECT test.id
					FROM executions
					INNER JOIN test ON test.id = executions.task_id
					WHERE executions.id = ?
				`, executionID)

				if err := row.Scan(&id); err != nil {
					if err == sql.ErrNoRows {
						shouldContinue = false
						log.Println("no more rows")
						return
					}

					log.Printf("scan id error: %v", err)
					return
				}

				break
			}

			log.Printf("got a row with id %s after %d attempts", id, retryCount)

			// simulating work...
			time.Sleep(1 * time.Second)

			// marking the row as completed
			if _, err := conn.ExecContext(ctx, "UPDATE test SET finished = TRUE WHERE id = ?", id); err != nil {
				log.Printf("update row error: %v", err)
				return
			}

			timeTaken := time.Now().Sub(startTimer)
			if timeTaken > maxTimeTaken {
				maxTimeTaken = timeTaken
			}

			return
		}()

		// still has enough time?
		if deadline, _ := ctx.Deadline(); deadline.Sub(time.Now()) < maxTimeTaken+3 {
			shouldContinue = false
		}
	}

	wg.Wait()

	// count the processed rows
	var count uint64

	row := conn.QueryRowContext(bgCtx, "SELECT COUNT(id) FROM test where finished = TRUE")

	if err := row.Scan(&count); err != nil {
		log.Printf("scan count error: %v", err)
		return
	}

	endedAt := time.Now().Sub(workStart)

	retryRate := (duplicatesCount * 100) / count

	log.Printf("%v rows processed in %v, %v%% retried, concurrency: %v, exiting", count, endedAt, retryRate, concurrency)
}

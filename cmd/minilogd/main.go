// Command minilogd is the one binary. It is a storage node, or an
// insert+select node, depending on whether -storageNode was passed.
//
// ---------------------------------------------------------------------------
// STAGE 6 -- YOUR IMPLEMENTATION GOES HERE.
//
// Verified by:  go run ./cmd/chaostest    (which spawns these)
//
//	and by hand, per the examples below.
//
// Reference: app/vlstorage/main.go.
// ---------------------------------------------------------------------------
//
// Storage node -- owns data, answers queries about its own shard:
//
//	./minilogd -storageDataPath=./data-1 -httpListenAddr=127.0.0.1:9001
//	./minilogd -storageDataPath=./data-2 -httpListenAddr=127.0.0.1:9002
//	./minilogd -storageDataPath=./data-3 -httpListenAddr=127.0.0.1:9003
//
// Insert+select node -- owns nothing, routes and fans out:
//
//	./minilogd -storageNode=127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003 \
//	           -httpListenAddr=127.0.0.1:9000
//
// One flag decides the role, and the same executable serves both. That is not
// a packaging convenience -- it is what makes "migrate a single node into a
// cluster" a config change rather than a data migration. A running single node
// becomes a storage node the moment somebody points a -storageNode list at it,
// with its existing data intact and immediately queryable through the new
// select node.
//
// Note what is NOT here: no cluster identity, no join protocol, no node
// registry, no health-check endpoint that peers consult. A storage node does
// not know it is in a cluster and cannot find out.
package main

import (
	"encoding/json"
	"flag"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/niladrix719/minilog/internalapi"
	"github.com/niladrix719/minilog/minilog"
	"github.com/niladrix719/minilog/netstorage"
)

func main() {
	storageNode := flag.String("storageNode", "", "storage node addr list")
	httpListenAddr := flag.String("httpListenAddr", "127.0.0.1:9000", "server addr")
	storageDataPath := flag.String("storageDataPath", "", "addr of the storage")
	flag.Parse()
	if *storageNode != "" {
		storageNodesList := strings.Split(*storageNode, ",")
		runInsertSelectNode(storageNodesList, *httpListenAddr)
	} else {
		runStorageNode(*storageDataPath, *httpListenAddr)
	}
}

// runStorageNode opens a local minilog.Storage at -storageDataPath and serves
// internalapi.NewStorageNodeHandler over -httpListenAddr.
//
// Handle SIGINT/SIGTERM: MustClose must run, or every unflushed in-memory part
// is lost. cmd/crashtest already taught you what an un-closed storage costs;
// this is the version of that lesson you control.
//
// Do NOT try to make SIGKILL survivable here. It is not, by design -- that is
// what cmd/crashtest measures and what a WAL would change.
func runStorageNode(dataPath, listenAddr string) {
	s := minilog.MustOpenStorage(dataPath, &minilog.Config{ShardsCount: 0, RetentionDays: 0})
	handler := internalapi.NewStorageNodeHandler(s)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		err := http.ListenAndServe(listenAddr, handler)
		if err != nil {
			log.Printf("storage node http server stopped %v", err)
		}
	}()
	<-sigCh
	s.MustClose()
}

type netStorageHandler struct {
	s *netstorage.Storage
}

func (n *netStorageHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var q minilog.Query
	var rows []minilog.Row
	var bytes []byte
	var resBytes []byte
	var err error
	switch r.Method {
	case "GET":
		bytes, err = io.ReadAll(r.Body)
		if err != nil {
			failedText := "failed to read request"
			http.Error(w, failedText, http.StatusBadRequest)
			return
		}
		err = json.Unmarshal(bytes, &q)
		if err != nil {
			failedText := "failed to unmarshal query"
			http.Error(w, failedText, http.StatusBadRequest)
			return
		}
		rows, ss := n.s.Search(&q)
		resp := struct {
			Rows  []minilog.Row        `json:"rows"`
			Stats *minilog.SearchStats `json:"stats"`
		}{Rows: rows, Stats: ss}
		resBytes, err = json.Marshal(resp)
		if err != nil {
			failedText := "failed to marshal response"
			http.Error(w, failedText, http.StatusBadRequest)
			return
		}
		_, err = w.Write(resBytes)
		if err != nil {
			log.Printf("failed to send response %v", err)
		}
	case "POST":
		bytes, err = io.ReadAll(r.Body)
		if err != nil {
			failedText := "failed to read request"
			http.Error(w, failedText, http.StatusBadRequest)
			return
		}
		err = json.Unmarshal(bytes, &rows)
		if err != nil {
			failedText := "failed to unmarshal rows"
			http.Error(w, failedText, http.StatusBadRequest)
			return
		}
		n.s.MustAddRows(rows)
		w.WriteHeader(http.StatusNoContent)
	default:
		http.NotFound(w, r)
		return
	}
}

// runInsertSelectNode builds a netstorage.Storage over the -storageNode
// addresses and serves a public API over -httpListenAddr.
//
// The public API is yours to keep minimal -- minilog has no query language and
// no ingestion protocols, so a JSON POST for insert and a JSON GET for query
// is plenty. Nothing in the harness depends on it; it exists so you can drive
// a real cluster by hand and watch it work, which is worth doing once before
// you trust the tests.
//
// Do not forget MustClose on shutdown here either, and note that it means
// something different on this side: the local node's close flushes to disk,
// this one drains buffered rows to other machines. Bound it -- a single
// unresponsive storage node must not hang your shutdown.
func runInsertSelectNode(storageNodeAddrs []string, listenAddr string) {
	s := netstorage.NewStorage(&netstorage.Config{Addrs: storageNodeAddrs})
	handler := &netStorageHandler{s: s}
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		err := http.ListenAndServe(listenAddr, handler)
		if err != nil {
			log.Printf("netstorage node http server stopped %v", err)
		}
	}()
	<-sigCh
	done := make(chan struct{})
	go func() {
		s.MustClose()
		close(done)
	}()
	select {
	case <-done:
		// close cleanly
	case <-time.After(5 * time.Second):
		log.Printf("netstorage close timeout after 5s; exiting anyway")
	}
}

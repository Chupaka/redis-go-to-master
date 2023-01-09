package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"time"

	systemdnotify "github.com/iguanesolutions/go-systemd/v5/notify"
)

var (
	masterAddr *net.TCPAddr

	nodes []string

	listenAddr = flag.String("listen", "", "listen address:port")
	redisNodes = flag.String("nodes", "", "comma-separated list of redis nodes")
	redisAuth  = flag.String("auth", "", "redis auth string")

	connectionsProxied int64 = 0
)

func main() {
	flag.Parse()

	if *listenAddr == "" {
		log.Fatalln("Must specify listen address!")
	}

	if *redisNodes == "" {
		log.Fatalln("Must specify at least one redis node!")
	}

	laddr, err := net.ResolveTCPAddr("tcp", *listenAddr)
	if err != nil {
		log.Fatalf("Failed to resolve listen address: %s\n", err)
	}

	nodes = strings.Split(*redisNodes, ",")

	log.Printf("Starting to watch the following redis servers: %s", strings.Join(nodes, ", "))

	go followMaster()

	listener, err := net.ListenTCP("tcp", laddr)
	if err != nil {
		log.Fatalf("Can't open listening socket: %s\n", err)
	}

	if err = systemdnotify.Ready(); err != nil {
		log.Printf("failed to notify ready to systemd: %v\n", err)
	}

	for {
		conn, err := listener.AcceptTCP()
		if err != nil {
			log.Printf("Can't accept connection: %s\n", err)
			continue
		}

		connectionsProxied += 1

		go proxy(conn, masterAddr)
	}
}

func followMaster() {
	for {
		newAddr := getMasterAddr()

		if newAddr == nil {
			log.Println("No masters found!")
		} else if masterAddr == nil || string(masterAddr.IP) != string(newAddr.IP) || masterAddr.Port != newAddr.Port {
			log.Printf("Changing master to %s:%d\n", newAddr.IP, newAddr.Port)
		}

		masterAddr = newAddr

		time.Sleep(1 * time.Second)

		systemdnotify.Status(fmt.Sprintf("Connections proxied: %d", connectionsProxied))
	}
}

func proxy(local io.ReadWriteCloser, remoteAddr *net.TCPAddr) {
	d := net.Dialer{Timeout: 1 * time.Second}
	remote, err := d.Dial("tcp", remoteAddr.String())
	if err != nil {
		log.Println(err)
		local.Close()
		return
	}

	go pipe(local, remote)
	go pipe(remote, local)
}

func pipe(r io.Reader, w io.WriteCloser) {
	io.Copy(w, r)
	w.Close()
}

func getMasterAddr() *net.TCPAddr {
	for _, node := range nodes {
		d := net.Dialer{Timeout: 1 * time.Second}
		conn, err := d.Dial("tcp", node)
		if err != nil {
			log.Printf("Can't connect to %s: %s\n", node, err)
			continue
		}

		defer conn.Close()

		if *redisAuth != "" {
			conn.Write([]byte(fmt.Sprintf("AUTH %s\r\ninfo replication\r\n", *redisAuth)))
		} else {
			conn.Write([]byte(fmt.Sprintf("info replication\r\n")))
		}

		b := make([]byte, 4096)

		l, err := conn.Read(b)
		if err != nil {
			log.Fatal(err)
		}

		if bytes.Contains(b[:l], []byte("role:master")) {
			return conn.RemoteAddr().(*net.TCPAddr)
		}

		if bytes.Contains(b[:l], []byte("-NOAUTH")) {
			log.Printf("%s: NOAUTH Authentication required\n", node)
		}
	}

	return nil
}

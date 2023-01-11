package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	systemdnotify "github.com/iguanesolutions/go-systemd/v5/notify"
)

type RedisPort struct {
	mutex      sync.RWMutex
	masterAddr *net.TCPAddr
	port       string
}

type Stats struct {
	connectionsProxied uint64
	pipesActive        uint32
}

var (
	nodes []string

	configPorts = flag.String("ports", "", "comma-eparated list of listening ports")
	configNodes = flag.String("nodes", "", "comma-separated list of redis nodes hostnames")
	configAuth  = flag.String("auth", "", "redis auth string")

	globalStats Stats
)

func main() {
	if systemdnotify.IsEnabled() {
		log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
	}

	flag.Parse()

	if *configPorts == "" {
		log.Fatalln("Must specify at least one listening port!")
	}

	if *configNodes == "" {
		log.Fatalln("Must specify at least one redis node!")
	}

	nodes = strings.Split(*configNodes, ",")
	log.Printf("Watching the following redis servers: %s", strings.Join(nodes, ", "))

	ports := strings.Split(*configPorts, ",")
	log.Printf("Serving the following ports: %s", strings.Join(ports, ", "))

	for _, port := range ports {
		go ServePort(port)
	}

	if err := systemdnotify.Ready(); err != nil {
		log.Printf("Failed to notify ready to systemd: %v\n", err)
	}

	ratePeriodStart := time.Now()
	rateProxiedValue := globalStats.connectionsProxied

	// just update systemd status time to time
	for {
		time.Sleep(time.Second * 5)

		var delta float64
		var rateProxied float64
		delta, ratePeriodStart = time.Since(ratePeriodStart).Seconds(), time.Now()
		rateProxied, rateProxiedValue = float64(globalStats.connectionsProxied-rateProxiedValue)/delta, globalStats.connectionsProxied

		statusString := fmt.Sprintf("Active connections: %d, proxied: %d, rate: %.1f/sec",
			globalStats.pipesActive/2,
			globalStats.connectionsProxied,
			rateProxied)

		if systemdnotify.IsEnabled() {
			systemdnotify.Status(statusString)
		} else {
			log.Println(statusString)
		}
	}
}

func ServePort(port string) {
	var p RedisPort

	p.port = port

	laddr, err := net.ResolveTCPAddr("tcp", ":"+port)
	if err != nil {
		log.Fatalf("Failed to resolve listen address: %s\n", err)
	}

	go followMaster(&p)

	listener, err := net.ListenTCP("tcp", laddr)
	if err != nil {
		log.Fatalf("Can't open listening socket for port %s: %s\n", port, err)
	}

	for {
		conn, err := listener.AcceptTCP()
		if err != nil {
			log.Printf("Can't accept connection on port %s: %s\n", port, err)
			continue
		}

		p.mutex.RLock()

		if p.masterAddr != nil {
			atomic.AddUint64(&globalStats.connectionsProxied, 1)
			go proxy(conn, p.masterAddr)
		} else {
			conn.Close()
		}

		p.mutex.RUnlock()
	}

}

func followMaster(rp *RedisPort) {
	for {
		newAddr := getMasterAddr(rp.port)

		if newAddr == nil {
			log.Printf("No masters found for port %s! Will not serve new connections until master is found...", rp.port)
		} else if rp.masterAddr == nil || string(rp.masterAddr.IP) != string(newAddr.IP) || rp.masterAddr.Port != newAddr.Port {
			log.Printf("Changing master to %s:%d\n", newAddr.IP, newAddr.Port)
		}

		rp.mutex.Lock()
		rp.masterAddr = newAddr
		rp.mutex.Unlock()

		time.Sleep(1 * time.Second)
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
	atomic.AddUint32(&globalStats.pipesActive, 1)
	io.Copy(w, r)
	w.Close()
	atomic.AddUint32(&globalStats.pipesActive, ^uint32(0))
}

func getMasterAddr(port string) *net.TCPAddr {
	for _, node := range nodes {
		d := net.Dialer{Timeout: 1 * time.Second}
		conn, err := d.Dial("tcp", node+":"+port)
		if err != nil {
			log.Printf("Can't connect to %s: %s\n", node, err)
			continue
		}

		defer conn.Close()

		if *configAuth != "" {
			conn.Write([]byte(fmt.Sprintf("AUTH %s\r\ninfo replication\r\n", *configAuth)))
		} else {
			conn.Write([]byte("info replication\r\n"))
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
			log.Printf("%s:%s: NOAUTH Authentication required\n", node, port)
		}
	}

	return nil
}

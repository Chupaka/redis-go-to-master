package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	systemdnotify "github.com/iguanesolutions/go-systemd/v5/notify"
	"gopkg.in/yaml.v2"
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

type ConfigStruct struct {
	Ports []string `yaml:"ports"`
	Nodes []string `yaml:"nodes"`
	Auth  string   `yaml:"auth"`
}

var (
	config ConfigStruct

	globalStats Stats
)

func main() {
	if systemdnotify.IsEnabled() {
		log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
	}

	if len(os.Args) != 2 {
		log.Fatalln("A single parameter is expected: config file name")
	}

	fn, err := filepath.Abs(os.Args[1])
	if err != nil {
		log.Fatalf("Can't get config file absolute path: %s\n", err)
	}

	log.Printf("Using onfiguration file %s\n", fn)

	f, err := os.Open(fn)
	if err != nil {
		log.Fatalf("Can't open config file: %s\n", err)
	}

	err = yaml.NewDecoder(f).Decode(&config)
	if err != nil {
		log.Fatalf("Can't read config: %s\n", err)
	}

	f.Close()

	if len(config.Ports) < 1 {
		log.Fatalln("Must specify at least one listening port!")
	}

	if len(config.Nodes) < 1 {
		log.Fatalln("Must specify at least one redis node!")
	}

	log.Printf("Watching the following redis servers: %s", strings.Join(config.Nodes, ", "))

	log.Printf("Serving the following ports: %s", strings.Join(config.Ports, ", "))

	for _, port := range config.Ports {
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
		var newAddr *net.TCPAddr
		for attempt := 1; newAddr == nil && attempt <= 3; attempt++ {
			newAddr = getMasterAddr(rp.port, attempt)
		}

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
	d := net.Dialer{Timeout: 3 * time.Second}
	remote, err := d.Dial("tcp", remoteAddr.String())
	if err != nil {
		log.Println(err)
		local.Close()
		return
	}

	local.(*net.TCPConn).SetKeepAlive(true)
	local.(*net.TCPConn).SetKeepAlivePeriod(5 * time.Second)

	remote.(*net.TCPConn).SetKeepAlive(true)
	remote.(*net.TCPConn).SetKeepAlivePeriod(5 * time.Second)

	go pipe(local, remote)
	go pipe(remote, local)
}

func pipe(r io.ReadCloser, w io.WriteCloser) {
	atomic.AddUint32(&globalStats.pipesActive, 1)                // increase by 1
	defer atomic.AddUint32(&globalStats.pipesActive, ^uint32(0)) // decrease by 1

	defer r.Close()
	defer w.Close()
	io.Copy(w, r)
}

func getMasterAddr(port string, timeout int) *net.TCPAddr {
	for _, node := range config.Nodes {
		d := net.Dialer{Timeout: time.Duration(timeout) * time.Second}
		conn, err := d.Dial("tcp", node+":"+port)
		if err != nil {
			log.Printf("Can't connect to %s with timeout %ds: %s\n", node, timeout, err)
			continue
		}

		defer conn.Close()

		if config.Auth != "" {
			conn.Write([]byte(fmt.Sprintf("AUTH %s\r\ninfo replication\r\n", config.Auth)))
		} else {
			conn.Write([]byte("info replication\r\n"))
		}

		b := make([]byte, 4096)

		l, err := conn.Read(b)
		if err != nil {
			log.Printf("Can't read Redis response: %s\n", err)
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

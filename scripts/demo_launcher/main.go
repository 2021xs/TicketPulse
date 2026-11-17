// DEMO-01 direct API parent. Only launches the official API; read switches
// and business writes remain in the existing CLI and HTTP contracts.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"ticketpulse/internal/mysqlstore"
	"ticketpulse/internal/projection"
	"ticketpulse/internal/readruntime"
)

type apiProcess struct {
	cmd      *exec.Cmd
	done     chan error
	identity readruntime.Process
	boot     string
	version  string
}
type launcher struct {
	env      map[string]string
	bin      string
	runtime  string
	evidence string
	mu       sync.Mutex
	api      *apiProcess
}
type request struct{ Action, Version string }

func main() {
	envFile := flag.String("environment", "", "private DEMO environment JSON")
	binRoot := flag.String("bin-root", "", "immutable private DEMO binary directory")
	flag.Parse()
	if err := run(*envFile, *binRoot); err != nil {
		fmt.Fprintln(os.Stderr, "DEMO launcher:", err)
		os.Exit(1)
	}
}
func run(envFile, binRoot string) error {
	raw, err := os.ReadFile(envFile)
	if err != nil {
		return err
	}
	var env map[string]string
	if err = json.Unmarshal(raw, &env); err != nil {
		return err
	}
	self, err := os.Executable()
	if err != nil {
		return err
	}
	self, err = filepath.EvalSymlinks(self)
	if err != nil {
		return err
	}
	binRoot, err = filepath.EvalSymlinks(binRoot)
	if err != nil || filepath.Dir(self) != binRoot || !strings.Contains(binRoot, string(os.PathSeparator)+".local/demo-01/") {
		return fmt.Errorf("launcher must be built in its private DEMO root")
	}
	if env["DEMO_API"] != "127.0.0.1:18101" || !strings.HasPrefix(env["DEMO_SCHEMA"], "ticketpulse_test_demo_") || env["TICKETPULSE_CDC_ADDR"] != "127.0.0.1:33316" {
		return fmt.Errorf("private DEMO resource identity differs")
	}
	l := &launcher{env: env, bin: binRoot, runtime: env["DEMO_RUNTIME"], evidence: env["DEMO_EVIDENCE"]}
	host, _ := os.Hostname()
	artifact := func(path string) (readruntime.Artifact, error) {
		h, e := readruntime.HashFile(path)
		return readruntime.Artifact{Path: path, SHA256: h}, e
	}
	parent, err := artifact(self)
	if err != nil {
		return err
	}
	scope := readruntime.Scope{Host: host, Root: binRoot, Launcher: parent, LaunchMode: "direct-no-restart", Exclusive: true}
	api, err := artifact(filepath.Join(binRoot, "ticketpulse"))
	if err != nil {
		return err
	}
	scope.Executables = []readruntime.Artifact{api}
	for _, name := range []string{"ticketpulse-sync", "ticketpulse-control", "ticketpulse-rebuild"} {
		a, e := artifact(filepath.Join(binRoot, name))
		if e != nil {
			return e
		}
		scope.OtherExecutables = append(scope.OtherExecutables, a)
	}
	if err = readruntime.CheckScope(context.Background(), scope); err != nil {
		return err
	}
	if err = save(filepath.Join(l.runtime, "scope.json"), scope); err != nil {
		return err
	}
	identity, err := readruntime.Inspect(context.Background(), os.Getpid(), "")
	if err != nil {
		return err
	}
	if err = save(filepath.Join(l.runtime, "launcher.json"), identity); err != nil {
		return err
	}
	socket := filepath.Join(l.runtime, "launcher.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		return err
	}
	defer listener.Close()
	if err = os.Chmod(socket, 0600); err != nil {
		return err
	}
	for {
		conn, e := listener.Accept()
		if e != nil {
			return e
		}
		go func() {
			defer conn.Close()
			_ = conn.SetDeadline(time.Now().Add(45 * time.Second))
			var r request
			if json.NewDecoder(conn).Decode(&r) != nil {
				return
			}
			l.mu.Lock()
			defer l.mu.Unlock()
			value, e := l.execute(r)
			if e != nil {
				value = map[string]any{"error": e.Error(), "action": r.Action}
			}
			_ = json.NewEncoder(conn).Encode(value)
		}()
	}
}
func save(path string, value any) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	e := json.NewEncoder(f)
	e.SetIndent("", "  ")
	return e.Encode(value)
}
func (l *launcher) execute(r request) (any, error) {
	switch r.Action {
	case "start":
		if l.api != nil {
			return nil, fmt.Errorf("an API is already tracked; stop and observe its exit first")
		}
		if r.Version != l.env["DEMO_A_VERSION"] && r.Version != l.env["DEMO_B_VERSION"] {
			return nil, fmt.Errorf("explicit A or B version required")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		db, err := mysqlstore.Open(ctx, l.env["TICKETPULSE_DSN"])
		if err != nil {
			return nil, err
		}
		v, err := (&projection.VersionStore{DB: db}).Get(ctx, r.Version)
		db.Close()
		if err != nil {
			return nil, err
		}
		cmd := exec.Command(filepath.Join(l.bin, "ticketpulse"), "-listen="+l.env["DEMO_API"], "-routing=configs/local.json")
		cmd.Dir, cmd.Env = filepath.Dir(filepath.Dir(filepath.Dir(l.runtime))), os.Environ()
		for key, value := range l.env {
			cmd.Env = append(cmd.Env, key+"="+value)
		}
		cmd.Env = append(cmd.Env, "TICKETPULSE_TARGET_VERSION="+r.Version, "TICKETPULSE_ES_URL="+v.Contract.ES.Endpoint, "TICKETPULSE_SEARCH_INDEX="+v.Contract.ES.Index, "TICKETPULSE_CK_URL="+v.Contract.CK.Endpoint, "TICKETPULSE_CK_DATABASE="+v.Contract.CK.Database)
		logPath := filepath.Join(l.evidence, fmt.Sprintf("api-%s-%d.log", r.Version, time.Now().UnixNano()))
		log, err := os.OpenFile(logPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return nil, err
		}
		cmd.Stdout, cmd.Stderr = log, log
		if err = cmd.Start(); err != nil {
			log.Close()
			return nil, err
		}
		p := &apiProcess{cmd: cmd, done: make(chan error, 1), version: r.Version}
		go func() { p.done <- cmd.Wait(); log.Close() }()
		p.identity, err = readruntime.Inspect(ctx, cmd.Process.Pid, l.env["DEMO_API"])
		if err != nil {
			return nil, err
		}
		l.api = p
		client := &http.Client{Timeout: 2 * time.Second}
		deadline := time.Now().Add(20 * time.Second)
		for time.Now().Before(deadline) {
			select {
			case e := <-p.done:
				l.api = nil
				return nil, fmt.Errorf("API exited before ready: %v; inspect %s", e, logPath)
			default:
			}
			res, e := client.Get("http://" + l.env["DEMO_API"] + "/readyz")
			if e == nil {
				res.Body.Close()
				if res.StatusCode == http.StatusOK {
					p.boot = res.Header.Get("X-TicketPulse-Boot-ID")
					return l.status(), nil
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
		return nil, fmt.Errorf("API tracked but not ready; no switch or resume performed")
	case "stop":
		if l.api == nil {
			return nil, fmt.Errorf("no tracked API; inspect persistent drain evidence")
		}
		p := l.api
		if err := readruntime.Alive(context.Background(), p.identity); err != nil {
			return nil, err
		}
		if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
			return nil, err
		}
		select {
		case err := <-p.done:
			if e := readruntime.Exited(context.Background(), p.identity); e != nil {
				return nil, e
			}
			l.api = nil
			return map[string]any{"identity": p.identity, "boot_id": p.boot, "actual_exit": true, "exit": fmt.Sprint(err)}, nil
		case <-time.After(20 * time.Second):
			return nil, fmt.Errorf("API exit not proven; maintain freeze and stop operation")
		}
	case "status":
		return l.status(), nil
	default:
		return nil, fmt.Errorf("only start, stop and status supported")
	}
}
func (l *launcher) status() any {
	if l.api == nil {
		return map[string]any{"api": nil, "automatic_restart": false}
	}
	return map[string]any{"api": l.api.identity, "boot_id": l.api.boot, "version": l.api.version, "automatic_restart": false}
}

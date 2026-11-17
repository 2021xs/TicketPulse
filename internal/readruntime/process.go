// Package readruntime contains only the bounded, same-host process observations
// used by read promotion. It neither launches nor signals a process.
package readruntime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
)

type Process struct {
	Host       string `json:"host"`
	PID        int    `json:"pid"`
	ParentPID  int    `json:"parent_pid"`
	Started    string `json:"started"`
	Executable string `json:"executable"`
	SHA256     string `json:"executable_sha256"`
	Listen     string `json:"listen"`
}
type Artifact struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

// Scope is an operator inventory, not a discovery platform. The first release
// requires exclusive control of this root and direct child launches. Declared
// launchd labels and disabled configuration files are checked again at commit.
type Scope struct {
	Host              string     `json:"host"`
	Root              string     `json:"root"`
	Executables       []Artifact `json:"executables"`
	OtherExecutables  []Artifact `json:"non_api_executables"`
	Launcher          Artifact   `json:"launcher"`
	LaunchMode        string     `json:"launch_mode"`
	LaunchdLabels     []string   `json:"disabled_launchd_labels"`
	DisabledArtifacts []Artifact `json:"disabled_artifacts"`
	Exclusive         bool       `json:"exclusive_operator_control"`
}

func HashFile(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:]), nil
}
func output(ctx context.Context, name string, args ...string) (string, error) {
	b, err := exec.CommandContext(ctx, name, args...).Output()
	return strings.TrimSpace(string(b)), err
}
func Inspect(ctx context.Context, pid int, listen string) (Process, error) {
	p := Process{PID: pid, Listen: listen}
	var err error
	if runtime.GOOS != "darwin" {
		return p, fmt.Errorf("READ_API_PROCESS_BLOCKED: process inspection is only verified on macOS")
	}
	if pid < 2 {
		return p, fmt.Errorf("invalid API PID")
	}
	p.Host, err = os.Hostname()
	if err != nil {
		return p, err
	}
	line, err := output(ctx, "ps", "-p", strconv.Itoa(pid), "-o", "ppid=,lstart=,comm=")
	if err != nil {
		return p, fmt.Errorf("API PID unavailable: %w", err)
	}
	fields := strings.Fields(line)
	if len(fields) < 7 {
		return p, fmt.Errorf("ambiguous API process identity")
	}
	p.ParentPID, err = strconv.Atoi(fields[0])
	if err != nil {
		return p, err
	}
	p.Started = strings.Join(fields[1:6], " ")
	p.Executable = strings.Join(fields[6:], " ")
	if !filepath.IsAbs(p.Executable) {
		return p, fmt.Errorf("API executable is not absolute")
	}
	p.Executable, err = filepath.EvalSymlinks(p.Executable)
	if err != nil {
		return p, err
	}
	p.SHA256, err = HashFile(p.Executable)
	return p, err
}
func Same(a, b Process) bool {
	return a.Host == b.Host && a.PID == b.PID && a.Started == b.Started && a.Executable == b.Executable && a.SHA256 == b.SHA256 && a.Listen == b.Listen
}
func Alive(ctx context.Context, p Process) error {
	actual, err := Inspect(ctx, p.PID, p.Listen)
	if err != nil {
		return err
	}
	if !Same(p, actual) {
		return fmt.Errorf("READ_API_PROCESS_CHANGED: PID/boot executable differs")
	}
	return CheckListener(ctx, p)
}
func CheckListener(ctx context.Context, p Process) error {
	text, err := output(ctx, "lsof", "-nP", "-a", "-p", strconv.Itoa(p.PID), "-iTCP", "-sTCP:LISTEN", "-Fn")
	if err != nil {
		return fmt.Errorf("API listener unavailable: %w", err)
	}
	for _, line := range strings.Split(text, "\n") {
		if line == "n"+p.Listen {
			return nil
		}
	}
	return fmt.Errorf("READ_API_PROCESS_CHANGED: expected listener absent")
}
func Listeners(ctx context.Context, pid int) ([]string, error) {
	text, err := output(ctx, "lsof", "-nP", "-a", "-p", strconv.Itoa(pid), "-iTCP", "-sTCP:LISTEN", "-Fn")
	if err != nil {
		return nil, err
	}
	var result []string
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(line, "n") {
			result = append(result, strings.TrimPrefix(line, "n"))
		}
	}
	return result, nil
}
func Exited(ctx context.Context, p Process) error {
	if err := syscall.Kill(p.PID, 0); err == syscall.ESRCH {
		return nil
	}
	if actual, err := Inspect(ctx, p.PID, p.Listen); err == nil && Same(p, actual) {
		return fmt.Errorf("READ_API_DRAIN_REQUIRED: PID %d still exists", p.PID)
	}
	return fmt.Errorf("READ_API_PROCESS_BLOCKED: PID %d not proven exited", p.PID)
}

// Scan returns sanitized executable identities only; arguments/environment can
// contain credentials and are never returned, persisted or logged.
func Scan(ctx context.Context, s Scope) ([]Process, error) {
	if err := CheckScope(ctx, s); err != nil {
		return nil, err
	}
	all, err := output(ctx, "ps", "-axo", "pid=,comm=")
	if err != nil {
		return nil, err
	}
	allowed := map[string]string{}
	other := map[string]string{s.Launcher.Path: s.Launcher.SHA256}
	for _, a := range s.Executables {
		allowed[a.Path] = a.SHA256
	}
	for _, a := range s.OtherExecutables {
		if _, exists := allowed[a.Path]; exists {
			return nil, fmt.Errorf("READ_API_PROCESS_BLOCKED: executable has ambiguous role")
		}
		other[a.Path] = a.SHA256
	}
	var result []Process
	for _, line := range strings.Split(all, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		name := strings.Join(fields[1:], " ")
		hash, declared := allowed[name]
		inRoot := strings.HasPrefix(name, s.Root+string(os.PathSeparator))
		if expected, ok := other[name]; ok {
			actual, err := HashFile(name)
			if err != nil || actual != expected {
				return nil, fmt.Errorf("READ_API_PROCESS_BLOCKED: non-API executable changed")
			}
			continue
		}
		if !declared && !inRoot {
			continue
		}
		if !declared {
			return nil, fmt.Errorf("READ_API_PROCESS_BLOCKED: undeclared API executable")
		}
		pid, err := strconv.Atoi(fields[0])
		if err != nil {
			return nil, err
		}
		p, err := Inspect(ctx, pid, "")
		if err != nil {
			return nil, err
		}
		if p.SHA256 != hash {
			return nil, fmt.Errorf("READ_API_PROCESS_BLOCKED: executable changed")
		}
		parent, err := Inspect(ctx, p.ParentPID, "")
		if err != nil || parent.Executable != s.Launcher.Path || parent.SHA256 != s.Launcher.SHA256 {
			return nil, fmt.Errorf("READ_API_PROCESS_BLOCKED: API has an unverified launcher/restart parent")
		}
		result = append(result, p)
	}
	return result, nil
}
func CheckScope(ctx context.Context, s Scope) error {
	host, err := os.Hostname()
	if err != nil {
		return err
	}
	if runtime.GOOS != "darwin" || s.Host != host || !filepath.IsAbs(s.Root) || !s.Exclusive || s.LaunchMode != "direct-no-restart" || len(s.Executables) == 0 {
		return fmt.Errorf("READ_API_PROCESS_BLOCKED: unsupported or incomplete host inventory")
	}
	real, err := filepath.EvalSymlinks(s.Root)
	if err != nil || real != s.Root {
		return fmt.Errorf("READ_API_PROCESS_BLOCKED: ambiguous scope root")
	}
	artifacts := append([]Artifact{s.Launcher}, s.Executables...)
	artifacts = append(artifacts, s.OtherExecutables...)
	artifacts = append(artifacts, s.DisabledArtifacts...)
	for _, a := range artifacts {
		if !strings.HasPrefix(a.Path, s.Root+string(os.PathSeparator)) || len(a.SHA256) != 64 {
			return fmt.Errorf("READ_API_PROCESS_BLOCKED: artifact outside controlled root")
		}
		h, err := HashFile(a.Path)
		if err != nil || h != a.SHA256 {
			return fmt.Errorf("READ_API_PROCESS_BLOCKED: launcher/executable/disabled config changed")
		}
	}
	// A declaration alone cannot disable a loaded supervisor. Reject any loaded
	// launchd job containing this root, including labels the inventory omitted.
	jobs, err := output(ctx, "launchctl", "print", "gui/"+strconv.Itoa(os.Getuid()))
	if err != nil {
		return fmt.Errorf("READ_API_PROCESS_BLOCKED: cannot inspect restart domain: %w", err)
	}
	if strings.Contains(jobs, s.Root) {
		return fmt.Errorf("READ_API_PROCESS_BLOCKED: controlled root has a loaded restart entry")
	}
	for _, label := range s.LaunchdLabels {
		if label == "" || strings.ContainsAny(label, "/\n ") {
			return fmt.Errorf("invalid restart label")
		}
		query := exec.CommandContext(ctx, "launchctl", "print", "gui/"+strconv.Itoa(os.Getuid())+"/"+label)
		data, err := query.CombinedOutput()
		if err == nil {
			return fmt.Errorf("READ_API_PROCESS_BLOCKED: restart entry still loaded")
		}
		if !strings.Contains(string(data), "Could not find service") {
			return fmt.Errorf("READ_API_PROCESS_BLOCKED: restart entry absence not proven")
		}
	}
	return nil
}

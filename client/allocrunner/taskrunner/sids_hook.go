package taskrunner

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"time"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/nomad/client/allocrunner/interfaces"
	"github.com/hashicorp/nomad/client/consul"
	"github.com/hashicorp/nomad/nomad/structs"
	"github.com/pkg/errors"
)

const (
	// the name of this hook, used in logs
	sidsHookName = "consul_sids"

	// sidsBackoffBaseline is the baseline time for exponential backoff when
	// attempting to retrieve a Consul SI token
	sidsBackoffBaseline = 5 * time.Second

	// sidsBackoffLimit is the limit of the exponential backoff when attempting
	// to retrieve a Consul SI token
	sidsBackoffLimit = 3 * time.Minute

	// sidsTokenFile is the name of the file holding the Consul SI token inside
	// the task's secret directory
	sidsTokenFile = "sids_token"

	// sidsTokenFilePerms is the level of file permissions granted on the file
	// in the secrets directory for the task
	sidsTokenFilePerms = 0440
)

type sidsHookConfig struct {
	alloc    *structs.Allocation
	task     *structs.Task
	siClient consul.ServiceIdentityAPI
	logger   hclog.Logger
}

// Consul Service Identity hook for managing SI tokens of connect enabled tasks.
type sidsHook struct {
	alloc    *structs.Allocation
	taskName string
	siClient consul.ServiceIdentityAPI
	logger   hclog.Logger

	lock     sync.Mutex
	firstRun bool
}

func newSIDSHook(c sidsHookConfig) *sidsHook {
	return &sidsHook{
		alloc:    c.alloc,
		taskName: c.task.Name,
		siClient: c.siClient,
		logger:   c.logger.Named(sidsHookName),
		firstRun: true,
	}
}

func (h *sidsHook) Name() string {
	return sidsHookName
}

func (h *sidsHook) Prestart(
	ctx context.Context,
	req *interfaces.TaskPrestartRequest,
	_ *interfaces.TaskPrestartResponse) error {

	h.lock.Lock()
	defer h.lock.Unlock()

	if h.earlyExit() {
		return nil
	}

	// try to recover token from disk
	token, err := h.recoverToken(req.TaskDir.SecretsDir)
	if err != nil {
		return err
	}

	if token == "" {
		if token, err = h.deriveSIToken(ctx); err != nil {
			return err
		}
		if err := h.writeToken(req.TaskDir.SecretsDir, token); err != nil {
			return err
		}
	}

	return nil
}

// earlyExit returns true if the Prestart hook has already been executed during
// the lifecycle of this task runner.
//
// assumes h is locked
func (h *sidsHook) earlyExit() bool {
	if h.firstRun {
		h.firstRun = false
		return false
	}
	return true
}

// recoverToken returns the token saved to disk in the secrets directory for the
// task if it exists, or the empty string if the file does not exist. an error
// is returned only for some other (e.g. disk IO) error.
func (h *sidsHook) recoverToken(dir string) (string, error) {
	tokenPath := filepath.Join(dir, sidsTokenFile)
	token, err := ioutil.ReadFile(tokenPath)
	if err != nil {
		if !os.IsNotExist(err) {
			return "", errors.Wrap(err, "failed to recover SI token")
		}
		h.logger.Trace("no pre-existing SI token to recover", "task", h.taskName)
		return "", nil // token file does not exist yet
	}
	h.logger.Trace("recovered pre-existing SI token", "task", h.taskName)
	return string(token), nil
}

func (h *sidsHook) deriveSIToken(ctx context.Context) (string, error) {
	tokenCh := make(chan string)

	// keep trying to get the token in the background
	go h.tryDerive(ctx, tokenCh)

	// wait until we get a token, or we get a signal to quit
	for {
		select {
		case token := <-tokenCh:
			return token, nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
}

func (h *sidsHook) tryDerive(ctx context.Context, ch chan<- string) {
	for i := 0; backoff(ctx, i); i++ {
		tokens, err := h.siClient.DeriveSITokens(h.alloc, []string{h.taskName})
		if err != nil {
			h.logger.Warn("failed to derive SI token", "attempt", i, "error", err)
			continue
		}
		ch <- tokens[h.taskName]
		return
	}
}

func backoff(ctx context.Context, i int) bool {
	next := computeBackoff(i)
	select {
	case <-ctx.Done():
		return false
	case <-time.After(next):
		return true
	}
}

func computeBackoff(attempt int) time.Duration {
	switch {
	case attempt == 0:
		return 0
	case attempt >= 4:
		return sidsBackoffLimit
	default:
		return (1 << (2 * uint(attempt))) * sidsBackoffBaseline
	}
}

func (h *sidsHook) writeToken(dir string, token string) error {
	tokenPath := filepath.Join(dir, sidsTokenFile)
	if err := ioutil.WriteFile(tokenPath, []byte(token), sidsTokenFilePerms); err != nil {
		return errors.Wrap(err, "failed to write SI token")
	}
	return nil
}

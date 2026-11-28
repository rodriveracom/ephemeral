// This test kills a process while it keeps writing, so the kill is not aligned
// with a return from the store. Every acknowledged call must be there afterwards,
// and at most the one call that followed may be there too. Each run found must
// show the state, position, value and history of a whole number of calls; lease
// tokens and times are not compared. It is a process kill, not a power loss.
// Where each kill lands is left to timing: no trial is known to have interrupted
// a transaction.
package ephemeral_test

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"syscall"
	"testing"
	"time"

	"github.com/rodriveracom/ephemeral"
)

const midwriteEnv = "EPHEMERAL_CONFORMANCE_MIDWRITE_DB"

// Each run takes three calls: StartOnce, Poll and Finish.
const callsPerRun = 3

func midwriteRun(i int) string { return fmt.Sprintf("r%04d", i) }

// writeUntilKilled prints one line after each call returns. The cap only stops a
// child whose parent has gone.
func writeUntilKilled(t *testing.T, path string) {
	s, e := ephemeral.Open(path)
	must(t, e)
	must(t, s.Register("job", 1, one))
	for i := range 900 {
		_, e = s.StartOnce(midwriteRun(i), "job", 1, "q", in, nil)
		must(t, e)
		fmt.Println("ack")
		task := lease(t, s, "q", time.Hour)
		fmt.Println("ack")
		must(t, s.Finish(task.Run, task.Token, done, ""))
		fmt.Println("ack")
	}
	t.Fatal("not killed")
}

// killAfter returns how many calls the child acknowledged before it died.
func killAfter(t *testing.T, path string, calls int, delay time.Duration) int {
	t.Helper()
	child := exec.Command(os.Args[0], "-test.run=^TestKillDuringWrites$")
	child.Env = append(os.Environ(), midwriteEnv+"="+path)
	var stderr bytes.Buffer
	child.Stderr = &stderr
	pipe, e := child.StdoutPipe()
	must(t, e)
	must(t, child.Start())
	t.Cleanup(func() { _ = child.Process.Kill() })

	out := bufio.NewReader(pipe)
	acked := 0
	killed := false
	for {
		line, e := out.ReadString('\n')
		if line == "ack\n" {
			acked++
		} else if line != "" {
			// Reap first, so that all of stderr has been copied.
			_ = child.Process.Kill()
			_ = child.Wait()
			t.Fatalf("child printed %q\n%s", line, &stderr)
		}
		if acked == calls && !killed {
			time.Sleep(delay)
			must(t, child.Process.Kill())
			killed = true
		}
		if errors.Is(e, io.EOF) {
			break
		}
		must(t, e)
	}
	e = child.Wait()
	var exit *exec.ExitError
	if !killed || !errors.As(e, &exit) || exit.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL {
		t.Fatalf("child was not killed after %d calls: %v\n%s", acked, e, &stderr)
	}
	return acked
}

// recordedCalls checks that everything compared about a run describes the same
// whole number of calls, and returns that number.
func recordedCalls(t *testing.T, s *ephemeral.Store, id string) int {
	t.Helper()
	d, e := s.Describe(id)
	if errors.Is(e, ephemeral.ErrMissing) {
		return 0
	}
	must(t, e)
	history, e := s.History(id, 0, 100)
	must(t, e)
	var got []string
	for _, event := range history {
		data := event.Data
		// The scheduled event also records a time, so keep only the name.
		if event.Kind == "ActivityScheduled" {
			var scheduled struct{ Name string }
			must(t, json.Unmarshal([]byte(data), &scheduled))
			data = scheduled.Name
		}
		got = append(got, fmt.Sprintf("%s %d %s", event.Kind, event.Step, data))
	}
	all := []string{
		"WorkflowStarted 0 " + string(in),
		"ActivityScheduled 0 call",
		"ActivityStarted 0 1",
		"ActivityCompleted 0 " + string(done),
		"WorkflowCompleted 1 " + string(done),
	}
	for calls, want := range []struct {
		state         string
		step, attempt int
		value         json.RawMessage
		events        int
	}{{"ready", 0, 0, in, 2}, {"leased", 0, 1, in, 3}, {"completed", 1, 1, done, 5}} {
		if d.State != want.state {
			continue
		}
		if d.ID != id || d.Name != "job" || d.Version != 1 || d.Queue != "q" || len(d.Attributes) != 0 ||
			d.Step != want.step || d.Attempt != want.attempt || !bytes.Equal(d.Value, want.value) ||
			!slices.Equal(got, all[:want.events]) {
			break
		}
		return calls + 1
	}
	t.Fatal("not a whole number of calls", d, got)
	return 0
}

func TestKillDuringWrites(t *testing.T) {
	if path := os.Getenv(midwriteEnv); path != "" {
		writeUntilKilled(t, path)
		return
	}
	extra := 0
	const trials = 20
	for trial := range trials {
		path := filepath.Join(t.TempDir(), "db")
		acked := killAfter(t, path, 1+trial*7, time.Duration(trial%5)*200*time.Microsecond)

		s := newStore(t, path)
		// The child may have started the run after the last acknowledged one, and
		// no run beyond that.
		runs := acked/callsPerRun + 2
		recorded := make([]int, runs)
		total := 0
		for i := range runs {
			recorded[i] = recordedCalls(t, s, midwriteRun(i))
			total += recorded[i]
		}
		if total != acked && total != acked+1 {
			t.Fatal("trial", trial, "acknowledged", acked, "recorded", total, recorded)
		}
		// The recorded calls are the first ones made, with no gap.
		var started []string
		for i := range runs {
			if want := max(0, min(callsPerRun, total-i*callsPerRun)); recorded[i] != want {
				t.Fatal("trial", trial, "run", i, "has", recorded[i], "calls, want", want, recorded)
			}
			if recorded[i] > 0 {
				started = append(started, midwriteRun(i))
			}
		}
		listed, e := s.List("", "", 1000)
		must(t, e)
		if !slices.Equal(listed, started) {
			t.Fatal("trial", trial, "listed", listed, "want", started)
		}
		extra += total - acked

		// The reopened store still takes writes.
		must(t, s.Start("after", "job", 1, "after", in, nil))
		must(t, s.Finish("after", lease(t, s, "after", time.Hour).Token, done, ""))
		state(t, s, "after", "completed")
		s.Close()
	}
	t.Logf("%d of %d trials kept one call the child had not acknowledged", extra, trials)
}

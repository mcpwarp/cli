package cli

import "testing"

func TestExitError(t *testing.T) {
	err := &exitError{code: 2}
	if err.ExitCode() != 2 {
		t.Errorf("got %d", err.ExitCode())
	}
	if err.Error() != "" {
		t.Errorf("expected empty message, got %q", err.Error())
	}
}

func TestRuntimeError(t *testing.T) {
	var ec ExitCoder
	err := RuntimeError()
	if ok := asExitCoder(err, &ec); !ok {
		t.Fatal("expected RuntimeError to implement ExitCoder")
	}
	if ec.ExitCode() != 1 {
		t.Errorf("got %d", ec.ExitCode())
	}
}

func asExitCoder(err error, out *ExitCoder) bool {
	ec, ok := err.(ExitCoder)
	if !ok {
		return false
	}
	*out = ec
	return true
}

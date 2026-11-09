package parser

import (
	"errors"
	"os"
	"strings"
	"testing"
)

func TestValidPipeline(t *testing.T) {
	yml := `
name: my-pipeline
triggers:
  - push
env:
  GO_ENV: test
jobs:
  - name: lint
    image: golangci/golangci-lint:latest
    steps:
      - name: run linter
        run: golangci-lint run ./...
  - name: test
    image: golang:1.22-alpine
    depends_on:
      - lint
    steps:
      - name: run tests
        run: go test ./...
`
	p, err := ParseBytes([]byte(yml))
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if p.Name != "my-pipeline" {
		t.Errorf("expected name %q, got %q", "my-pipeline", p.Name)
	}
	if len(p.Jobs) != 2 {
		t.Errorf("expected 2 jobs, got %d", len(p.Jobs))
	}
	if p.Env["GO_ENV"] != "test" {
		t.Errorf("expected env GO_ENV=test, got %q", p.Env["GO_ENV"])
	}
	if len(p.Jobs[1].DependsOn) != 1 || p.Jobs[1].DependsOn[0] != "lint" {
		t.Errorf("expected job 'test' to depend on 'lint'")
	}
}

func TestMissingPipelineName(t *testing.T) {
	yml := `
jobs:
  - name: build
    image: golang:1.22-alpine
    steps:
      - name: compile
        run: go build ./...
`
	_, err := ParseBytes([]byte(yml))
	if err == nil {
		t.Fatal("expected error for missing pipeline name")
	}
	var pe *ParseError
	if !errors.As(err, &pe) {
		t.Fatalf("expected ParseError, got %T: %v", err, err)
	}
	if pe.Field != "name" {
		t.Errorf("expected field %q, got %q", "name", pe.Field)
	}
}

func TestJobWithNoSteps(t *testing.T) {
	yml := `
name: bad-pipeline
jobs:
  - name: empty-job
    image: alpine:latest
`
	_, err := ParseBytes([]byte(yml))
	if err == nil {
		t.Fatal("expected error for job with no steps")
	}
	var pe *ParseError
	if !errors.As(err, &pe) {
		t.Fatalf("expected ParseError, got %T: %v", err, err)
	}
	if !strings.Contains(pe.Message, "at least one step") {
		t.Errorf("expected message about missing steps, got: %s", pe.Message)
	}
}

func TestCircularDependency(t *testing.T) {
	yml := `
name: circular
jobs:
  - name: a
    image: alpine
    depends_on:
      - b
    steps:
      - name: step-a
        run: echo a
  - name: b
    image: alpine
    depends_on:
      - a
    steps:
      - name: step-b
        run: echo b
`
	_, err := ParseBytes([]byte(yml))
	if err == nil {
		t.Fatal("expected error for circular dependency")
	}
	msg := err.Error()
	if !strings.Contains(msg, "circular dependency detected") {
		t.Errorf("expected circular dependency error, got: %s", msg)
	}
	if !strings.Contains(msg, "a") || !strings.Contains(msg, "b") {
		t.Errorf("expected cycle to mention both jobs, got: %s", msg)
	}
}

func TestUnknownDependsOn(t *testing.T) {
	yml := `
name: bad-dep
jobs:
  - name: build
    image: golang:1.22-alpine
    depends_on:
      - nonexistent
    steps:
      - name: compile
        run: go build ./...
`
	_, err := ParseBytes([]byte(yml))
	if err == nil {
		t.Fatal("expected error for unknown depends_on reference")
	}
	msg := err.Error()
	if !strings.Contains(msg, "unknown job") {
		t.Errorf("expected unknown job error, got: %s", msg)
	}
	if !strings.Contains(msg, "nonexistent") {
		t.Errorf("expected error to mention %q, got: %s", "nonexistent", msg)
	}
}

func TestDuplicateJobNames(t *testing.T) {
	yml := `
name: dup
jobs:
  - name: build
    image: alpine
    steps:
      - name: s1
        run: echo 1
  - name: build
    image: alpine
    steps:
      - name: s2
        run: echo 2
`
	_, err := ParseBytes([]byte(yml))
	if err == nil {
		t.Fatal("expected error for duplicate job names")
	}
	var pe *ParseError
	if !errors.As(err, &pe) {
		t.Fatalf("expected ParseError, got %T: %v", err, err)
	}
	if !strings.Contains(pe.Message, "duplicate") {
		t.Errorf("expected duplicate error, got: %s", pe.Message)
	}
}

func TestParseBytesWithExampleConfig(t *testing.T) {
	data, err := os.ReadFile("../../configs/example.drev.yml")
	if err != nil {
		t.Fatalf("reading example config: %v", err)
	}
	p, err := ParseBytes(data)
	if err != nil {
		t.Fatalf("expected no error parsing example config, got: %v", err)
	}
	if p.Name != "drev-ci-pipeline" {
		t.Errorf("expected name %q, got %q", "drev-ci-pipeline", p.Name)
	}
	if len(p.Jobs) != 2 {
		t.Fatalf("expected 2 jobs, got %d", len(p.Jobs))
	}
	if p.Jobs[0].Name != "test" {
		t.Errorf("expected first job %q, got %q", "test", p.Jobs[0].Name)
	}
	if p.Jobs[1].Name != "build" {
		t.Errorf("expected second job %q, got %q", "build", p.Jobs[1].Name)
	}
	if len(p.Jobs[1].DependsOn) != 1 || p.Jobs[1].DependsOn[0] != "test" {
		t.Error("expected build job to depend on test")
	}
	if p.Env["APP_NAME"] != "drev" {
		t.Errorf("expected APP_NAME=drev, got %q", p.Env["APP_NAME"])
	}
}

func TestValidJobTimeout(t *testing.T) {
	yml := `
name: timeout-pipeline
jobs:
  - name: quick-job
    image: alpine:latest
    timeout: 30s
    steps:
      - name: step1
        run: echo hi
  - name: long-job
    image: alpine:latest
    timeout: 5m
    steps:
      - name: step2
        run: echo bye
`
	p, err := ParseBytes([]byte(yml))
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if p.Jobs[0].Timeout != "30s" {
		t.Errorf("expected timeout '30s', got %q", p.Jobs[0].Timeout)
	}
	if p.Jobs[1].Timeout != "5m" {
		t.Errorf("expected timeout '5m', got %q", p.Jobs[1].Timeout)
	}
}

func TestInvalidJobTimeoutFormat(t *testing.T) {
	yml := `
name: bad-timeout
jobs:
  - name: job1
    image: alpine:latest
    timeout: not-a-duration
    steps:
      - name: step1
        run: echo hi
`
	_, err := ParseBytes([]byte(yml))
	if err == nil {
		t.Fatal("expected error for invalid timeout format")
	}
	var pe *ParseError
	if !errors.As(err, &pe) {
		t.Fatalf("expected ParseError, got %T: %v", err, err)
	}
	if pe.Field != "jobs[0].timeout" {
		t.Errorf("expected field 'jobs[0].timeout', got %q", pe.Field)
	}
	if !strings.Contains(pe.Message, "invalid timeout") {
		t.Errorf("expected invalid timeout message, got: %s", pe.Message)
	}
}

func TestJobTimeoutZero(t *testing.T) {
	yml := `
name: zero-timeout
jobs:
  - name: job1
    image: alpine:latest
    timeout: 0s
    steps:
      - name: step1
        run: echo hi
`
	_, err := ParseBytes([]byte(yml))
	if err == nil {
		t.Fatal("expected error for zero timeout")
	}
	var pe *ParseError
	if !errors.As(err, &pe) {
		t.Fatalf("expected ParseError, got %T: %v", err, err)
	}
	if pe.Field != "jobs[0].timeout" {
		t.Errorf("expected field 'jobs[0].timeout', got %q", pe.Field)
	}
	if !strings.Contains(pe.Message, "greater than zero") {
		t.Errorf("expected greater than zero message, got: %s", pe.Message)
	}
}

func TestJobTimeoutNegative(t *testing.T) {
	yml := `
name: negative-timeout
jobs:
  - name: job1
    image: alpine:latest
    timeout: -5s
    steps:
      - name: step1
        run: echo hi
`
	_, err := ParseBytes([]byte(yml))
	if err == nil {
		t.Fatal("expected error for negative timeout")
	}
	var pe *ParseError
	if !errors.As(err, &pe) {
		t.Fatalf("expected ParseError, got %T: %v", err, err)
	}
	if pe.Field != "jobs[0].timeout" {
		t.Errorf("expected field 'jobs[0].timeout', got %q", pe.Field)
	}
	if !strings.Contains(pe.Message, "greater than zero") {
		t.Errorf("expected greater than zero message, got: %s", pe.Message)
	}
}

func TestJobWithoutTimeout(t *testing.T) {
	yml := `
name: no-timeout
jobs:
  - name: job1
    image: alpine:latest
    steps:
      - name: step1
        run: echo hi
`
	p, err := ParseBytes([]byte(yml))
	if err != nil {
		t.Fatalf("expected no error for job without timeout, got: %v", err)
	}
	if p.Jobs[0].Timeout != "" {
		t.Errorf("expected empty timeout, got %q", p.Jobs[0].Timeout)
	}
}

package parser

import (
	"fmt"
	"os"
	"strings"

	"github.com/drevci/drev/pkg/drevtypes"
	"gopkg.in/yaml.v3"
)

// Parser handles pipeline configuration parsing.
type Parser struct{}

func NewParser() *Parser { return &Parser{} }

// ParseError represents a validation error in a pipeline definition.
type ParseError struct {
	Field   string
	Message string
}

func (e *ParseError) Error() string {
	if e.Field != "" {
		return fmt.Sprintf("%s: %s", e.Field, e.Message)
	}
	return e.Message
}

// ParseFile reads a .drev.yml file from disk and returns a validated Pipeline.
func ParseFile(path string) (*drevtypes.Pipeline, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading pipeline file: %w", err)
	}
	return ParseBytes(data)
}

// ParseBytes parses raw YAML bytes into a validated Pipeline.
func ParseBytes(data []byte) (*drevtypes.Pipeline, error) {
	var p drevtypes.Pipeline
	if err := yaml.Unmarshal(data, &p); err != nil {
		return nil, fmt.Errorf("parsing YAML: %w", err)
	}
	if err := Validate(&p); err != nil {
		return nil, err
	}
	return &p, nil
}

// Validate checks a Pipeline for structural and semantic correctness.
func Validate(p *drevtypes.Pipeline) error {
	if p.Name == "" {
		return &ParseError{Field: "name", Message: "pipeline must have a name"}
	}

	if len(p.Jobs) == 0 {
		return &ParseError{Field: "jobs", Message: "pipeline must have at least one job"}
	}

	jobNames := make(map[string]bool)
	for i, job := range p.Jobs {
		if job.Name == "" {
			return &ParseError{
				Field:   fmt.Sprintf("jobs[%d].name", i),
				Message: "job must have a name",
			}
		}
		if jobNames[job.Name] {
			return &ParseError{
				Field:   fmt.Sprintf("jobs[%d].name", i),
				Message: fmt.Sprintf("duplicate job name %q", job.Name),
			}
		}
		jobNames[job.Name] = true

		if job.Image == "" {
			return &ParseError{
				Field:   fmt.Sprintf("jobs[%d].image", i),
				Message: fmt.Sprintf("job %q must have an image", job.Name),
			}
		}

		if len(job.Steps) == 0 {
			return &ParseError{
				Field:   fmt.Sprintf("jobs[%d].steps", i),
				Message: fmt.Sprintf("job %q must have at least one step", job.Name),
			}
		}

		for j, step := range job.Steps {
			if step.Name == "" {
				return &ParseError{
					Field:   fmt.Sprintf("jobs[%d].steps[%d].name", i, j),
					Message: fmt.Sprintf("step in job %q must have a name", job.Name),
				}
			}
			if step.Run == "" {
				return &ParseError{
					Field:   fmt.Sprintf("jobs[%d].steps[%d].run", i, j),
					Message: fmt.Sprintf("step %q in job %q must have a run command", step.Name, job.Name),
				}
			}
		}
	}

	for _, job := range p.Jobs {
		for _, dep := range job.DependsOn {
			if !jobNames[dep] {
				return &ParseError{
					Field:   "depends_on",
					Message: fmt.Sprintf("job %q depends on unknown job %q", job.Name, dep),
				}
			}
		}
	}

	if err := detectCycle(p.Jobs); err != nil {
		return err
	}

	return nil
}

func detectCycle(jobs []drevtypes.Job) error {
	graph := make(map[string][]string)
	for _, j := range jobs {
		graph[j.Name] = j.DependsOn
	}

	const (
		white = 0
		gray  = 1
		black = 2
	)

	color := make(map[string]int)
	var path []string

	var dfs func(node string) error
	dfs = func(node string) error {
		color[node] = gray
		path = append(path, node)

		for _, dep := range graph[node] {
			if color[dep] == gray {
				cycleStart := -1
				for i, n := range path {
					if n == dep {
						cycleStart = i
						break
					}
				}
				cyclePath := make([]string, len(path[cycleStart:]))
				copy(cyclePath, path[cycleStart:])
				cyclePath = append(cyclePath, dep)
				return &ParseError{
					Field:   "depends_on",
					Message: fmt.Sprintf("circular dependency detected: %s", strings.Join(cyclePath, " -> ")),
				}
			}
			if color[dep] == white {
				if err := dfs(dep); err != nil {
					return err
				}
			}
		}

		path = path[:len(path)-1]
		color[node] = black
		return nil
	}

	for _, j := range jobs {
		if color[j.Name] == white {
			if err := dfs(j.Name); err != nil {
				return err
			}
		}
	}

	return nil
}

package runner

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// ResourceLimits specifies constraints enforced on job containers.
type ResourceLimits struct {
	NanoCPUs   int64 // CPU quota in units of 10^-9 CPUs (e.g. 2*1e9 = 2.0 CPUs). Must be > 0.
	Memory     int64 // Memory limit in bytes (minimum 64MB). Must be > 0.
	MemorySwap int64 // Total memory + swap limit in bytes (-1 for unlimited, == Memory for swap disabled).
	PidsLimit  int64 // Max concurrent processes in container. Must be > 0.
}

const (
	// DefaultNanoCPUs allocates 2.0 CPU cores per container.
	DefaultNanoCPUs = int64(2 * 1e9)
	// DefaultMemory allocates 2GB of RAM per container.
	DefaultMemory = int64(2 * 1024 * 1024 * 1024)
	// DefaultMemorySwap equals DefaultMemory, disabling additional swap usage by default.
	// In Docker, MemorySwap represents total memory plus swap. When MemorySwap == Memory,
	// the container is not allowed to use any additional swap on disk.
	DefaultMemorySwap = DefaultMemory
	// DefaultPidsLimit limits containers to 512 concurrent processes to prevent fork bombs.
	DefaultPidsLimit = int64(512)
)

// DefaultResourceLimits returns the standard production defaults for self-hosted CI jobs.
func DefaultResourceLimits() ResourceLimits {
	return ResourceLimits{
		NanoCPUs:   DefaultNanoCPUs,
		Memory:     DefaultMemory,
		MemorySwap: DefaultMemorySwap,
		PidsLimit:  DefaultPidsLimit,
	}
}

// Validate verifies that the resource limits are non-contradictory and within valid ranges.
func (r ResourceLimits) Validate() error {
	if r.NanoCPUs <= 0 {
		return fmt.Errorf("invalid CPU limit (%d NanoCPUs): must be greater than 0", r.NanoCPUs)
	}
	if r.Memory < 64*1024*1024 {
		return fmt.Errorf("invalid memory limit (%d bytes): must be at least 64MB (67108864 bytes)", r.Memory)
	}
	if r.MemorySwap != -1 && r.MemorySwap < r.Memory {
		return fmt.Errorf("invalid memory-swap limit (%d bytes): must be -1 (unlimited) or greater than/equal to memory limit (%d bytes)", r.MemorySwap, r.Memory)
	}
	if r.PidsLimit <= 0 {
		return fmt.Errorf("invalid pids limit (%d): must be greater than 0", r.PidsLimit)
	}
	return nil
}

// LoadResourceLimitsFromEnv reads operator overrides from environment variables
// and falls back to DefaultResourceLimits for any unset parameters.
func LoadResourceLimitsFromEnv() (ResourceLimits, error) {
	limits := DefaultResourceLimits()

	if s := os.Getenv("DREV_JOB_CPU_LIMIT"); s != "" {
		cpu, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
		if err != nil || cpu <= 0 {
			return limits, fmt.Errorf("invalid DREV_JOB_CPU_LIMIT %q: must be a positive number (e.g. '2' or '1.5')", s)
		}
		limits.NanoCPUs = int64(cpu * 1e9)
	}

	if s := os.Getenv("DREV_JOB_MEMORY_LIMIT"); s != "" {
		mem, err := parseByteSize(s)
		if err != nil {
			return limits, fmt.Errorf("invalid DREV_JOB_MEMORY_LIMIT %q: %w", s, err)
		}
		limits.Memory = mem
		limits.MemorySwap = mem // default swap to match memory unless DREV_JOB_SWAP_LIMIT is specified
	}

	if s := os.Getenv("DREV_JOB_SWAP_LIMIT"); s != "" {
		s = strings.TrimSpace(strings.ToLower(s))
		if s == "0" || s == "none" || s == "0b" || s == "0mb" || s == "0gb" {
			limits.MemorySwap = limits.Memory
		} else if s == "-1" || s == "unlimited" {
			limits.MemorySwap = -1
		} else {
			swapAdditional, err := parseByteSize(s)
			if err != nil {
				return limits, fmt.Errorf("invalid DREV_JOB_SWAP_LIMIT %q: %w", s, err)
			}
			limits.MemorySwap = limits.Memory + swapAdditional
		}
	}

	if s := os.Getenv("DREV_JOB_PIDS_LIMIT"); s != "" {
		pids, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
		if err != nil || pids <= 0 {
			return limits, fmt.Errorf("invalid DREV_JOB_PIDS_LIMIT %q: must be a positive integer", s)
		}
		limits.PidsLimit = pids
	}

	if err := limits.Validate(); err != nil {
		return limits, fmt.Errorf("validating resource limits: %w", err)
	}

	return limits, nil
}

func parseByteSize(s string) (int64, error) {
	s = strings.TrimSpace(strings.ToUpper(s))
	if s == "" {
		return 0, errors.New("empty size string")
	}

	var multiplier int64 = 1
	var numStr string

	if strings.HasSuffix(s, "G") || strings.HasSuffix(s, "GB") {
		multiplier = 1024 * 1024 * 1024
		numStr = strings.TrimSuffix(strings.TrimSuffix(s, "B"), "G")
	} else if strings.HasSuffix(s, "M") || strings.HasSuffix(s, "MB") {
		multiplier = 1024 * 1024
		numStr = strings.TrimSuffix(strings.TrimSuffix(s, "B"), "M")
	} else if strings.HasSuffix(s, "K") || strings.HasSuffix(s, "KB") {
		multiplier = 1024
		numStr = strings.TrimSuffix(strings.TrimSuffix(s, "B"), "K")
	} else if strings.HasSuffix(s, "B") {
		multiplier = 1
		numStr = strings.TrimSuffix(s, "B")
	} else {
		numStr = s
	}

	numStr = strings.TrimSpace(numStr)
	val, err := strconv.ParseFloat(numStr, 64)
	if err != nil || val <= 0 {
		return 0, fmt.Errorf("invalid numeric value in byte size: %s", s)
	}

	return int64(val * float64(multiplier)), nil
}

func formatSwapLimit(r ResourceLimits) string {
	if r.MemorySwap == -1 {
		return "unlimited"
	}
	if r.MemorySwap == r.Memory {
		return "0MB (disabled)"
	}
	return fmt.Sprintf("%dMB", (r.MemorySwap-r.Memory)/(1024*1024))
}

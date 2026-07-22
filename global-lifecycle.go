// Copyright (c) 2026 MinIO, Inc.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or (at your
// option) any later version.

package madmin

/* trinet */

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"path"
	"time"
)

const globalLifecycleAPI = "global-lifecycle"

const (
	GlobalLifecycleRuleEnabled  = "Enabled"
	GlobalLifecycleRuleDisabled = "Disabled"
)

// GlobalLifecycleDuration is encoded as a Go duration string in JSON.
type GlobalLifecycleDuration time.Duration

func (d GlobalLifecycleDuration) Duration() time.Duration {
	return time.Duration(d)
}

func (d GlobalLifecycleDuration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

func (d *GlobalLifecycleDuration) UnmarshalJSON(data []byte) error {
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return errors.New("monitorInterval must be a duration string")
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return err
	}
	*d = GlobalLifecycleDuration(parsed)
	return nil
}

type GlobalLifecycleConfig struct {
	Enabled             bool                    `json:"enabled"`
	CapacityTrigger     float64                 `json:"capacityTrigger"`
	CapacityStop        float64                 `json:"capacityStop"`
	MonitorInterval     GlobalLifecycleDuration `json:"monitorInterval"`
	Rules               []GlobalLifecycleRule   `json:"rules"`
	Generation          uint64                  `json:"generation"`
	UpdatedAt           *time.Time              `json:"updatedAt"`
	CapacityActivatedAt *time.Time              `json:"capacityActivatedAt"`
}

type GlobalLifecycleRule struct {
	ID                          string                     `json:"id"`
	Status                      string                     `json:"status"`
	Filter                      GlobalLifecycleFilter      `json:"filter"`
	AccessTime                  GlobalLifecycleAccessTime  `json:"accessTime"`
	Transition                  *GlobalLifecycleTransition `json:"transition,omitempty"`
	NoncurrentVersionTransition *GlobalLifecycleTransition `json:"noncurrentVersionTransition,omitempty"`
}

type GlobalLifecycleFilter struct {
	Prefix string            `json:"prefix"`
	Tags   map[string]string `json:"tags,omitempty"`
}

type GlobalLifecycleAccessTime struct {
	MinimumMinutes int `json:"minimumMinutes"`
}

type GlobalLifecycleTransition struct {
	StorageClass string `json:"storageClass"`
}

// DefaultGlobalLifecycleConfig returns the same disabled defaults used by the
// server when no configuration object has been persisted.
func DefaultGlobalLifecycleConfig() GlobalLifecycleConfig {
	return GlobalLifecycleConfig{
		CapacityTrigger: .8,
		CapacityStop:    .7,
		MonitorInterval: GlobalLifecycleDuration(5 * time.Minute),
		Rules:           []GlobalLifecycleRule{},
	}
}

type globalLifecyclePutRequest struct {
	Enabled         bool                    `json:"enabled"`
	CapacityTrigger float64                 `json:"capacityTrigger"`
	CapacityStop    float64                 `json:"capacityStop"`
	MonitorInterval GlobalLifecycleDuration `json:"monitorInterval"`
	Rules           []GlobalLifecycleRule   `json:"rules"`
	Generation      *uint64                 `json:"generation,omitempty"`
}

// SetGlobalLifecycle replaces the complete global lifecycle configuration.
// expectedGeneration may point to zero for the first conditional update. A
// nil value performs an unconditional replacement.
func (adm *AdminClient) SetGlobalLifecycle(ctx context.Context, cfg GlobalLifecycleConfig, expectedGeneration *uint64) error {
	request := globalLifecyclePutRequest{
		Enabled:         cfg.Enabled,
		CapacityTrigger: cfg.CapacityTrigger,
		CapacityStop:    cfg.CapacityStop,
		MonitorInterval: cfg.MonitorInterval,
		Rules:           cfg.Rules,
		Generation:      expectedGeneration,
	}
	data, err := json.Marshal(request)
	if err != nil {
		return err
	}
	resp, err := adm.executeMethod(ctx, http.MethodPut, requestData{
		relPath: path.Join(adminAPIPrefix, globalLifecycleAPI),
		content: data,
	})
	defer closeResponse(resp)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusNoContent {
		return httpRespToErrorResponse(resp)
	}
	return nil
}

// GetGlobalLifecycle returns the active configuration. A server without a
// persisted configuration returns the disabled defaults at generation zero.
func (adm *AdminClient) GetGlobalLifecycle(ctx context.Context) (GlobalLifecycleConfig, error) {
	resp, err := adm.executeMethod(ctx, http.MethodGet, requestData{
		relPath: path.Join(adminAPIPrefix, globalLifecycleAPI),
	})
	defer closeResponse(resp)
	if err != nil {
		return GlobalLifecycleConfig{}, err
	}
	if resp.StatusCode != http.StatusOK {
		return GlobalLifecycleConfig{}, httpRespToErrorResponse(resp)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return GlobalLifecycleConfig{}, err
	}
	var cfg GlobalLifecycleConfig
	if err = json.Unmarshal(data, &cfg); err != nil {
		return GlobalLifecycleConfig{}, err
	}
	if cfg.Rules == nil {
		cfg.Rules = []GlobalLifecycleRule{}
	}
	return cfg, nil
}

// DeleteGlobalLifecycle disables capacity lifecycle and persists a new
// generation tombstone on the server.
func (adm *AdminClient) DeleteGlobalLifecycle(ctx context.Context) error {
	resp, err := adm.executeMethod(ctx, http.MethodDelete, requestData{
		relPath: path.Join(adminAPIPrefix, globalLifecycleAPI),
	})
	defer closeResponse(resp)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusNoContent {
		return httpRespToErrorResponse(resp)
	}
	return nil
}

/* trinet */

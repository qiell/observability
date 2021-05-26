package values

import (
	"time"
)

type Result struct {
	Time          time.Time         `json:"timestamp"`
	Event         EventType         `json:"event_type"`
	Successful    bool              `json:"successful,omitempty"`
	Bucket        string            `json:"bucket,omitempty"`
	BucketType    string            `json:"bucket_type,omitempty"`
	Node          string            `json:"node,omitempty"`
	Service       string            `json:"service,omitempty"`
	Dataset       string            `json:"dataset,omitempty"`
	Index         string            `json:"index,omitempty"`
	Function      string            `json:"function,omitempty"`
	Task          string            `json:"task_name,omitempty"`
	Group         string            `json:"group,omitempty"`
	Groups        []string          `json:"groups,omitempty"`
	SourceBucket  string            `json:"source_bucket,omitempty"`
	TargetBucket  string            `json:"target_bucket,omitempty"`
	Cluster       string            `json:"cluster,omitempty"`
	Ticks         int               `json:"dropped_ticks,omitempty"`
	Reason        string            `json:"reason,omitempty"`
	NodesIn       []string          `json:"nodes_in,omitempty"`
	NodesOut      []string          `json:"nodes_out,omitempty"`
	Repo          string            `json:"backup_repository,omitempty"`
	Backup        string            `json:"backup_name,omitempty"`
	User          string            `json:"user,omitempty"`
	DataLost      int               `json:"percent_data_lost,omitempty"`
	Plan          string            `json:"plan,omitempty"`
	OldRepository string            `json:"old_repository,omitempty"`
	NewRepository string            `json:"new_repository,omitempty"`
	OperationID   string            `json:"operation_id,omitempty"`
	Settings      map[string]string `json:"settings,omitempty"`
}
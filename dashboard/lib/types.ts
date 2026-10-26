export type RunStatus = 'pending' | 'running' | 'success' | 'failed' | 'cancelled';

export interface Run {
  id: string;
  pipeline_id: string;
  pipeline_name?: string;
  status: RunStatus;
  triggered_by?: string;
  commit_sha?: string;
  commit_msg?: string;
  branch?: string;
  started_at: string;
  finished_at: string;
  created_at?: string;
  author?: string;
  jobs?: RunJob[];
}

export interface RunJob {
  id: string;
  run_id: string;
  job_name: string;
  status: RunStatus;
  started_at: string;
  finished_at: string;
}

export interface OverviewStats {
  queue_depth: number;
  worker_capacity: number;
  queue_status: string;
  sample_limit: number;
  completed_sample_count: number;
  duration_sample_count: number;
  succeeded_count: number;
  failed_count: number;
  cancelled_count: number;
  pass_rate: number | null;
  mean_duration_sec: number | null;
  recent_failures: Run[];
  active_runs_count: number;
  total_runs_count: number;
}

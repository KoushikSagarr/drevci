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

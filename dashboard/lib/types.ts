export type RunStatus = 'pending' | 'running' | 'success' | 'failed' | 'cancelled';

export interface Run {
  id: string;
  pipeline_id: string;
  status: RunStatus;
  started_at: string;
  finished_at: string;
}

export interface RunJob {
  id: string;
  run_id: string;
  job_name: string;
  status: RunStatus;
  started_at: string;
  finished_at: string;
}

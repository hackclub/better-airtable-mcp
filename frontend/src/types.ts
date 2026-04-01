export type OperationStatus =
  | "pending_approval"
  | "approved"
  | "rejected"
  | "expired"
  | "executing"
  | "completed"
  | "partially_completed"
  | "failed";

export interface OperationPreviewRecord {
  id?: string;
  fields?: Record<string, unknown>;
  current_fields?: Record<string, unknown>;
}

export interface OperationPreview {
  type: "create_records" | "update_records" | "delete_records";
  table: string;
  original_table_name?: string;
  records: OperationPreviewRecord[];
}

export interface ExecutionResult {
  created_record_ids?: string[];
  updated_record_ids?: string[];
  deleted_record_ids?: string[];
  completed_batches: number;
  failed_batch?: number;
}

export interface OperationView {
  operation_id: string;
  status: OperationStatus;
  approval_url: string;
  base_id: string;
  base_name: string;
  mcp_session_id?: string;
  mcp_client_id?: string;
  mcp_client_name?: string;
  summary: string;
  created_at: string;
  expires_at: string;
  resolved_at?: string;
  last_synced_at: string;
  operations: OperationPreview[];
  result?: ExecutionResult;
  error?: string;
  approval_url_is_credential: boolean;
  preview_is_snapshot: boolean;
  can_approve: boolean;
  can_reject: boolean;
}

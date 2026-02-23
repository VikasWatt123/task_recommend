export interface Employee {
  employee_code: string;
  employee_name: string;
  technical_skills: {
    structural_design?: string[];
    electrical_design?: string[];
    coordination?: string[];
  };
  raw_technical_skills?: string[]; // Added missing property
  skills?: string[];
  normalized_skills?: string[];
  normalized_expertise?: string[];
  current_role: string;
  shift?: string;
  experience_years?: number;
  current_experience_years?: number; // Added missing property
  availability?: string;
  status_1?: string;
  status_2?: string;
  status_3?: string;
  reporting_manager?: string;
  "List of task assigned"?: string;
  list_of_task_assigned?: string;
  "Special Task"?: string;
  special_task?: string;
  team_lead_id?: string;
  contact_email?: string;
  employee_status?: {
    availability: string;
  };
}

export interface StageHistoryEntry {
  stage: string;
  status: 'completed' | 'in_progress' | 'pending';
  assigned_to?: {
    employee_code: string;
    employee_name: string;
    current_role: string;
  } | null;
  created_at: string;
  assigned_at?: string;
  started_at?: string;
  completed_at?: string;
  duration_minutes?: number;
}

export interface FileStageHistory {
  success: boolean;
  file_id: string;
  original_filename: string;
  current_stage: string;
  current_status: string;
  stage_history: StageHistoryEntry[];
  total_stages: number;
  created_at: string;
  updated_at: string;
}

export interface EmployeeWithEmbeddings extends Employee {
  embeddings?: {
    skill_embedding?: number[];
    profile_embedding?: number[];
  };
  embedding?: number[]; // Single embedding array
  // Task-related fields (optional for Employee type)
  current_tasks?: Array<{
    task_id: string;
    title?: string;
    description?: string;
    status: string;
    assigned_at: string;
    due_date?: string;
    skills_required?: string[];
  }>;
  active_task_count?: number;
  total_task_count?: number;
}

export interface TaskTemplate {
  task_id: string;
  task_type: string;
  task_description?: string;
  required_skills: string[];
  difficulty_level?: 'LOW' | 'MEDIUM' | 'HIGH';
  ideal_time_minutes?: number;
  max_time_minutes?: number;
}

export interface PermitFile {
  _id: string; // This will be file_id from backend
  file_id: string;
  file_name: string; // Added missing property
  file_type: 'NEW' | 'REVISION';
  status: string;
  current_step?: string; // Added missing property
  workflow_step?: 'PRELIMS' | 'PRODUCTION' | 'QC'; // Added workflow step
  state?: string;
  client?: string;
  created_at: string;
  updated_at: string;
  file_size?: number;
  file_path?: string;
  uploaded_by?: string;
  current_assignment?: {
    employee_code: string;
    employee_name: string;
    assigned_at: string;
    stage: string;
    started_at?: string;
  };
  metadata?: {
    uploaded_by: string;
    created_at: string;
    updated_at: string;
  };
  file_info?: {
    original_filename: string;
    stored_filename: string;
    file_path: string;
    file_size: number;
    mime_type: string;
    uploaded_at: string; // Added upload timestamp
  };
  project_details?: {
    client_name: string;
    project_name: string;
  };
  assigned_to_lead?: string;
  /** @deprecated Use current_assignment instead */
  assignment?: {
    assigned_to: string;
    assigned_at: string;
    assigned_for_stage: string;
    assigned_by: string;
  };
}

export interface PermitFileTask {
  _id: string;
  permit_file_code: string;
  step: 'PRELIMS' | 'PRODUCTION' | 'QC';
  task_id?: string;
  title?: string;
  status: 'OPEN' | 'ASSIGNED' | 'IN_PROGRESS' | 'DONE' | 'CANCELLED';
  assigned_to_employee_code?: string;
  assigned_by?: string;
  started_at?: string;
  completed_at?: string;
  actual_time_minutes?: number;
}

export interface Recommendation {
  employee_code: string;
  employee_name: string;
  similarity_score?: number;
  match_percentage?: number;
  technical_skills: {
    structural_design?: string[];
    electrical_design?: string[];
    coordination?: string[];
  };
  skills_match?: {
    structural_design?: string[];
    electrical_design?: string[];
    coordination?: string[];
  }; // Skills from recommendation engine
  skills?: string[]; // For backward compatibility
  normalized_skills?: string[]; // Clean normalized skills
  normalized_expertise?: string[]; // Clean normalized expertise
  current_role: string;
  shift?: string;
  experience_years?: number;
  availability?: string;
  reasoning: string;
  task_relevance?: string;
  current_task_count?: number; // Added for task load display
  current_tasks?: Array<{
    task_id: string;
    title?: string;
    description?: string;
    status: string;
    assigned_at: string;
    due_date?: string;
    skills_required?: string[];
  }>;
  active_task_count?: number;
  total_task_count?: number;
}

export type WorkflowStep = 'PRELIMS' | 'PRODUCTION' | 'QC';
export type TaskStatus = 'OPEN' | 'ASSIGNED' | 'IN_PROGRESS' | 'DONE' | 'CANCELLED';
export type PermitStatus = 'PENDING' | 'IN_PRELIMS' | 'IN_PRODUCTION' | 'IN_QC' | 'ON_HOLD' | 'DELIVERED' | 'DONE';

export interface TeamLeadGroup {
  team_lead_code: string;
  team_lead_name: string;
  team_size?: number;
  employees: Employee[];
  team_lead_info?: unknown;
}

export interface Task {
  _id: string;
  employee_code: string;
  employee_name?: string;
  task_description: string; // For frontend display
  task_assigned: string; // From backend
  title?: string; // Alternative title field
  description?: string; // Alternative description field
  status: 'OPEN' | 'ASSIGNED' | 'IN_PROGRESS' | 'DONE' | 'COMPLETED';
  assigned_at: string;
  time_assigned: string;
  date_assigned: string;
  assigned_by: string;
  assigned_to?: string; // Standardized assigned employee field
  task_id: string;
  created_at: string;
  updated_at: string;
  completion_time?: string; // For completed tasks
  completed_at?: string; // Alternative completion time field
  completion_notes?: string; // Optional completion notes
  hours_taken?: number; // Hours taken to complete
  // New dual-flow tracking fields
  file_id?: string; // Primary file identifier
  tracking_mode?: 'FILE_BASED' | 'STANDALONE'; // Task tracking mode
  stage?: string; // Current stage for file-based tasks
  // Enhanced fields from permit file integration
  /** @deprecated Use file_id instead */
  permit_file_id?: string;
  client_name?: string;
  project_name?: string;
  original_filename?: string;
  skills_required?: string[];
  // Backward compatibility
  source?: {
    file_id?: string;
    permit_file_id?: string;
  };
}

export interface TaskAssignment {
  tasks: Task[];
  total: number;
}

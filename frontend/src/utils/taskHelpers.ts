/**
 * Task Helper Utilities
 * Provides backward compatibility for task and file field transitions
 * Handles migration from permit_file_id to file_id
 */

export type TrackingMode = 'FILE_BASED' | 'STANDALONE';

/**
 * Extracts file_id from various possible field locations
 * Provides backward compatibility with permit_file_id
 */
export function getFileId(item: any): string | null {
  if (!item) return null;
  
  return (
    item.file_id ||
    item.permit_file_id ||
    item.source?.permit_file_id ||
    item.source?.file_id ||
    null
  );
}

/**
 * Determines if a task is standalone (not associated with a file)
 */
export function isStandaloneTask(task: any): boolean {
  if (!task) return false;
  
  if (task.tracking_mode === 'STANDALONE') return true;
  if (task.tracking_mode === 'FILE_BASED') return false;
  
  return !getFileId(task);
}

/**
 * Gets tracking mode with fallback logic
 */
export function getTrackingMode(task: any): TrackingMode {
  if (!task) return 'STANDALONE';
  
  if (task.tracking_mode) {
    return task.tracking_mode as TrackingMode;
  }
  
  return getFileId(task) ? 'FILE_BASED' : 'STANDALONE';
}

/**
 * Normalizes task object to use new field names while maintaining backward compatibility
 */
export function normalizeTask(task: any): any {
  if (!task) return task;
  
  const fileId = getFileId(task);
  const trackingMode = getTrackingMode(task);
  
  return {
    ...task,
    file_id: fileId,
    tracking_mode: trackingMode,
    assigned_to: task.assigned_to || task.assignment || task.employee_code,
  };
}

/**
 * Normalizes array of tasks
 */
export function normalizeTasks(tasks: any[]): any[] {
  if (!Array.isArray(tasks)) return [];
  return tasks.map(normalizeTask);
}

/**
 * Gets display-friendly file identifier
 */
export function getFileDisplayName(item: any): string {
  const fileId = getFileId(item);
  
  if (!fileId) return 'Standalone Task';
  
  if (item.file_name) return item.file_name;
  if (item.original_filename) return item.original_filename;
  
  return fileId;
}

/**
 * Extracts assignment information from permit file with backward compatibility
 */
export interface AssignmentInfo {
  assignedTo: string;
  assignedToName?: string;
  assignedAt: Date;
  stage: string;
}

export function getAssignmentInfo(file: any): AssignmentInfo | null {
  if (!file) return null;
  
  // Try new standardized format first
  if (file.current_assignment) {
    return {
      assignedTo: file.current_assignment.employee_code,
      assignedToName: file.current_assignment.employee_name,
      assignedAt: new Date(file.current_assignment.assigned_at),
      stage: file.current_assignment.stage,
    };
  }
  
  // Fallback to old assignment format
  if (file.assignment) {
    return {
      assignedTo: file.assignment.assigned_to,
      assignedToName: undefined,
      assignedAt: new Date(file.assignment.assigned_at),
      stage: file.assignment.assigned_for_stage,
    };
  }
  
  return null;
}

/**
 * Gets assigned employee code with fallback
 */
export function getAssignedTo(task: any): string | null {
  if (!task) return null;
  
  return (
    task.assigned_to ||
    task.assignment ||
    task.employee_code ||
    task.assigned_to_employee_code ||
    null
  );
}

/**
 * Formats tracking mode for display
 */
export function formatTrackingMode(mode: TrackingMode): string {
  return mode === 'FILE_BASED' ? 'File-Based' : 'Standalone';
}

/**
 * Gets tracking mode badge color
 */
export function getTrackingModeBadgeVariant(mode: TrackingMode): 'default' | 'secondary' {
  return mode === 'FILE_BASED' ? 'default' : 'secondary';
}

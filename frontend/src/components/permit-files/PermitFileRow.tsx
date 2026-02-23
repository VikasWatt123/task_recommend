import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { PermitFile } from "@/types";
import { Eye, FileText, Calendar, MapPin, Building2, User, BarChart3, Clock, Users, AlertTriangle } from "lucide-react";
import { cn } from "@/lib/utils";
import { formatDistanceToNow } from "date-fns";
import { Link } from "react-router-dom";
import { useState } from "react";
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogDescription } from "@/components/ui/dialog";
import { Skeleton } from "@/components/ui/skeleton";
import { api } from "@/lib/api";
import { getAssignmentInfo } from "@/utils/taskHelpers";

interface PermitFileRowProps {
  file: PermitFile;
  index?: number;
}

const statusConfig: Record<string, { label: string; variant: "success" | "warning" | "destructive" | "secondary" | "default" }> = {
  PENDING: { label: "Pending", variant: "secondary" },
  PENDING_REVIEW: { label: "Pending Review", variant: "secondary" },
  IN_PRELIMS: { label: "In Prelims", variant: "warning" },
  IN_PRODUCTION: { label: "In Production", variant: "warning" },
  IN_QC: { label: "In QC", variant: "default" },
  ON_HOLD: { label: "On Hold", variant: "destructive" },
  DELIVERED: { label: "Delivered", variant: "success" },
  DONE: { label: "Done", variant: "success" },
};

export function PermitFileRow({ file, index = 0 }: PermitFileRowProps) {
  const status = statusConfig[file.status] || { label: file.status || 'Unknown', variant: 'secondary' as const };
  const [showReport, setShowReport] = useState(false);
  const [reportData, setReportData] = useState<any>(null);
  const [loadingReport, setLoadingReport] = useState(false);
  
  // Get the correct date from metadata or file_info
  const getDate = () => {
    if (file.metadata?.updated_at) {
      return new Date(file.metadata.updated_at);
    } else if (file.file_info?.uploaded_at) {
      return new Date(file.file_info.uploaded_at);
    } else {
      return new Date(); // Fallback
    }
  };
  
  // Use helper function for assignment info
  const assignmentInfo = getAssignmentInfo(file);
  
  // Load detailed report
  const loadReport = async () => {
    setLoadingReport(true);
    try {
      const { data } = await api.get<unknown>(`/permit-files/${file.file_id}/completion-report`);
      setReportData(data);
    } catch (error) {
      console.error('[PermitFileRow] Failed to load report:', error);
      setReportData(null);
    } finally {
      setLoadingReport(false);
    }
  };
  
  const handleShowReport = () => {
    setShowReport(true);
    if (!reportData) {
      loadReport();
    }
  };
  
  return (
    <>
      <div 
        className="group flex items-center gap-4 p-4 rounded-lg bg-card border border-border hover:border-primary/30 transition-all duration-200 animate-slide-up"
        style={{ animationDelay: `${index * 50}ms` }}
      >
      {/* Icon */}
      <div className="p-3 rounded-lg bg-primary/10">
        <FileText className="h-5 w-5 text-primary" />
      </div>
      
      {/* Main Info */}
      <div className="flex-1 min-w-0 grid grid-cols-1 md:grid-cols-5 gap-2 md:gap-4">
        <div>
          <p className="font-semibold text-sm truncate" title={file.file_name || file.file_id}>
            {file.file_name || `File-${file.file_id}`}
          </p>
          <p className="text-xs text-muted-foreground font-mono">{file.file_id}</p>
        </div>
        
        <div className="flex items-center gap-1.5 text-sm text-muted-foreground">
          <MapPin className="h-3.5 w-3.5" />
          <span>{file.workflow_step || file.status}</span>
        </div>
        
        <div className="flex items-center gap-1.5 text-sm text-muted-foreground">
          <Building2 className="h-3.5 w-3.5" />
          <span className="truncate" title={file.project_details?.client_name || 'N/A'}>
            {file.project_details?.client_name || 'N/A'}
          </span>
        </div>
        
        {assignmentInfo ? (
          <div className="flex items-center gap-1.5 text-sm text-muted-foreground">
            <User className="h-3.5 w-3.5" />
            <div className="truncate">
              <span className="font-medium">
                {assignmentInfo.assignedToName || `#${assignmentInfo.assignedTo}`}
              </span>
              <span className="text-xs block">{assignmentInfo.stage}</span>
            </div>
          </div>
        ) : (
          <div className="flex items-center gap-1.5 text-sm text-muted-foreground">
            <User className="h-3.5 w-3.5" />
            <span>Unassigned</span>
          </div>
        )}
        
        <div className="flex items-center gap-1.5 text-sm text-muted-foreground">
          <Calendar className="h-3.5 w-3.5" />
          <span>{formatDistanceToNow(getDate(), { addSuffix: true })}</span>
        </div>
      </div>
      
      {/* Status */}
      <Badge variant={status.variant}>{status.label}</Badge>
      
      {/* Actions */}
      <div className="flex items-center gap-1 opacity-0 group-hover:opacity-100 transition-opacity">
        <Button 
          variant="ghost" 
          size="icon-sm" 
          onClick={handleShowReport}
          title="View Detailed Report"
        >
          <BarChart3 className="h-4 w-4" />
        </Button>
        <Button 
          variant="ghost" 
          size="icon-sm" 
          asChild
          title="View File Details"
        >
          <Link to={`/permit-files/${file.file_id}`}>
            <Eye className="h-4 w-4" />
          </Link>
        </Button>
      </div>
    </div>
    
    {/* Detailed Report Dialog */}
    <Dialog open={showReport} onOpenChange={setShowReport}>
      <DialogContent className="max-w-4xl max-h-[80vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <BarChart3 className="h-5 w-5" />
            Completion Report - {file.file_name || file.file_id}
          </DialogTitle>
          <DialogDescription>
            Detailed view of stage completion times, assigned employees, and task summary for this permit file.
          </DialogDescription>
        </DialogHeader>
        
        {loadingReport ? (
          <div className="space-y-4">
            <Skeleton className="h-4 w-32" />
            <Skeleton className="h-20 w-full" />
            <div className="grid grid-cols-3 gap-4">
              <Skeleton className="h-24 w-full" />
              <Skeleton className="h-24 w-full" />
              <Skeleton className="h-24 w-full" />
            </div>
          </div>
        ) : reportData ? (
          <div className="space-y-6">
            {/* Summary Cards */}
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
              <div className="p-4 rounded-lg bg-blue-50 border border-blue-200">
                <div className="flex items-center gap-2 text-blue-700 mb-1">
                  <FileText className="h-4 w-4" />
                  <span className="text-sm font-medium">Current Stage</span>
                </div>
                <p className="text-2xl font-bold text-blue-900">{reportData.current_stage}</p>
              </div>
              
              <div className="p-4 rounded-lg bg-green-50 border border-green-200">
                <div className="flex items-center gap-2 text-green-700 mb-1">
                  <Clock className="h-4 w-4" />
                  <span className="text-sm font-medium">Total Duration</span>
                </div>
                <p className="text-2xl font-bold text-green-900">
                  {Math.floor(reportData.total_duration_minutes / 60)}h {reportData.total_duration_minutes % 60}m
                </p>
              </div>
              
              <div className="p-4 rounded-lg bg-purple-50 border border-purple-200">
                <div className="flex items-center gap-2 text-purple-700 mb-1">
                  <Users className="h-4 w-4" />
                  <span className="text-sm font-medium">Tasks</span>
                </div>
                <p className="text-2xl font-bold text-purple-900">
                  {reportData.task_summary.completed_tasks || 0}/{reportData.task_summary.total_tasks || 0}
                </p>
                <p className="text-xs text-purple-600 mt-1">
                  Active: {reportData.task_summary.active_tasks || 0}
                </p>
              </div>
              
              <div className="p-4 rounded-lg bg-red-50 border border-red-200">
                <div className="flex items-center gap-2 text-red-700 mb-1">
                  <AlertTriangle className="h-4 w-4" />
                  <span className="text-sm font-medium">SLA Breaches</span>
                </div>
                <p className="text-2xl font-bold text-red-900">
                  {reportData.sla_summary?.total_breaches || 0}
                </p>
                <p className="text-xs text-red-600 mt-1">
                  Penalties: {reportData.sla_summary?.total_penalties || 0}
                </p>
              </div>
            </div>
            
            {/* Stage Timeline */}
            <div>
              <h3 className="text-lg font-semibold mb-3 flex items-center gap-2">
                <Clock className="h-5 w-5" />
                Stage Timeline
              </h3>
              <div className="space-y-3">
                {reportData.stage_timeline.map((stage: any, idx: number) => (
                  <div key={idx} className="flex items-center gap-4 p-3 rounded-lg bg-gray-50 border">
                    <div className="w-32">
                      <Badge variant="outline">{stage.stage}</Badge>
                    </div>
                    <div className="flex-1">
                      <p className="font-medium">{stage.employee_name || 'Unassigned'}</p>
                      <p className="text-sm text-gray-600">
                        {stage.started_at ? new Date(stage.started_at).toLocaleString() : 'Not started'}
                        {stage.completed_at && ` - ${new Date(stage.completed_at).toLocaleString()}`}
                      </p>
                    </div>
                    <div className="text-right">
                      <p className="text-sm font-medium">{stage.duration_minutes} min</p>
                      <p className="text-xs text-gray-500">
                        {stage.completed_at ? 'Completed' : 'In Progress'}
                      </p>
                    </div>
                  </div>
                ))}
              </div>
            </div>
            
            {/* Task Summary by Stage */}
            <div>
              <h3 className="text-lg font-semibold mb-3 flex items-center gap-2">
                <Users className="h-5 w-5" />
                Tasks by Stage
              </h3>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                {Object.entries(reportData.task_summary.by_stage).map(([stage, data]: [string, any]) => (
                  <div key={stage} className="p-4 rounded-lg border">
                    <div className="flex items-center justify-between mb-2">
                      <Badge variant="outline">{stage}</Badge>
                      <span className="text-sm text-gray-600">
                        {data.completed}/{data.total} tasks
                      </span>
                    </div>
                    <div className="space-y-2">
                      {Object.entries(data.employees).map(([empCode, empData]: [string, any]) => (
                        <div key={empCode} className="text-sm">
                          <p className="font-medium">{empData.employee_name}</p>
                          <p className="text-gray-600">{empData.tasks.length} tasks assigned</p>
                        </div>
                      ))}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </div>
        ) : (
          <div className="text-center py-8">
            <p className="text-gray-500">No report data available</p>
          </div>
        )}
      </DialogContent>
    </Dialog>
    </>
  );
}

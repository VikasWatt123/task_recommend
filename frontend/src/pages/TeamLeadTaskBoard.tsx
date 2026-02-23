import { 
  KanbanSquare, 
  Users, 
  FileText, 
  CheckCircle, 
  CheckCircle2,
  Clock, 
  AlertCircle, 
  Loader2, 
  RefreshCw, 
  ChevronDown, 
  ChevronUp,
  User,
  Calendar,
  Target
} from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { useToast } from "@/hooks/use-toast";
import { useNavigate } from "react-router-dom";
import { useState, useEffect } from "react";
import { getTeamLeadTaskStats, getPermitFileTracking, completeTask } from "@/lib/api";
import { getFileId, getFileDisplayName } from "@/utils/taskHelpers";

// Helper function to format dates with proper timezone handling
const formatDate = (dateString: string | undefined | null): string => {
  if (!dateString) return 'No date';
  
  try {
    // Handle different date formats
    let date: Date;
    if (dateString.includes('T')) {
      // ISO format with timezone
      if (dateString.endsWith('Z')) {
        date = new Date(dateString);
      } else {
        date = new Date(dateString + 'Z');
      }
    } else {
      // Other formats
      date = new Date(dateString);
    }
    
    return date.toLocaleDateString() + ' at ' + date.toLocaleTimeString();
  } catch (error) {
    return 'Invalid date';
  }
};

// Updated interfaces to match the new API responses
interface TeamTaskItem {
  employee_code: string;
  employee_name: string;
  employee_role: string;
  task_count: number;
  tasks: Array<{
    task_id: string;
    task_title: string;
    status: string;
    assigned_at: string;
    completed_at?: string;
  }>;
}

interface PermitTaskItem {
  task_id: string;
  task_title: string;
  status: string;
  assigned_to: string;
  employee_name: string;
  employee_role: string;
  team_lead?: string;
  assigned_at: string;
  completed_at?: string;
}

interface TeamLeadStats {
  team_lead_code: string;
  team_lead_name: string;
  total_tasks: number;
  completed_tasks: number;
  in_progress_tasks: number;
  assigned_tasks: number;
  completion_rate: number;
  unique_employees: number;  // Actual unique employee count
  employees: TeamTaskItem[];
}

interface PermitFile {
  file_id: string;
  permit_file_id?: string; // Backward compatibility
  file_name?: string;
  total_tasks: number;
  completed_tasks: number;
  in_progress_tasks: number;
  assigned_tasks: number;
  completion_rate: number;
  status: string;
  tasks: PermitTaskItem[];
}

export default function TeamLeadTaskBoard() {
  const isDebugEnabled = (() => {
    try {
      return localStorage.getItem('teamLeadDebug') === 'true';
    } catch {
      return false;
    }
  })();

  const [teamStats, setTeamStats] = useState<TeamLeadStats[]>([]);
  const [permitFiles, setPermitFiles] = useState<PermitFile[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [lastRefresh, setLastRefresh] = useState<Date>(new Date());
  const [expandedTeams, setExpandedTeams] = useState<Set<string>>(new Set());
  const [expandedFiles, setExpandedFiles] = useState<Set<string>>(new Set());
  const navigate = useNavigate();
  const { toast } = useToast();

  useEffect(() => {
    loadTeamLeadData(true); // Initial load with loading indicator
    
    // Set up polling for real-time updates (every 60 seconds - reduced from 30s)
    const interval = setInterval(() => {
      loadTeamLeadData(false); // Silent refresh without loading indicator
    }, 60000);
    
    return () => clearInterval(interval);
  }, []);

  const loadTeamLeadData = async (showLoading = true) => {
    if (showLoading) setIsLoading(true);
    try {
      // Load team lead stats and permit file tracking in parallel
      const [teamData, permitData] = await Promise.all([
        getTeamLeadTaskStats(),
        getPermitFileTracking()
      ]);
      
      // Backend returns team_lead_stats (not team_stats), flatten task_statistics into each item
      const rawStats = (teamData as any)?.team_lead_stats || (teamData as any)?.team_stats || [];
      const mappedStats: TeamLeadStats[] = rawStats.map((item: any) => ({
        team_lead_code: item.team_lead_code,
        team_lead_name: item.team_lead_name,
        total_tasks: item.task_statistics?.total_tasks ?? item.total_tasks ?? 0,
        completed_tasks: item.task_statistics?.completed_tasks ?? item.completed_tasks ?? 0,
        in_progress_tasks: item.task_statistics?.pending_tasks ?? item.in_progress_tasks ?? 0,
        assigned_tasks: item.task_statistics?.total_tasks ?? item.assigned_tasks ?? 0,
        completion_rate: item.task_statistics?.completion_rate ?? item.completion_rate ?? 0,
        unique_employees: item.total_employees ?? item.unique_employees ?? 0,
        employees: item.employees || [],
      }));
      setTeamStats(mappedStats);
      setPermitFiles((permitData?.data || []) as PermitFile[]);
      setLastRefresh(new Date());

      if (isDebugEnabled) {
        console.log(`Loaded ${mappedStats.length} teams and ${permitData?.total_permit_files || 0} permit files`);
      }
    } catch (error) {
      console.error('Error loading team lead data:', error);
      // Ensure state remains arrays even on error
      setTeamStats([]);
      setPermitFiles([]);
    } finally {
      if (showLoading) setIsLoading(false);
    }
  };

  const handleCompleteTask = async (taskId: string, employeeCode: string) => {
    try {
      await completeTask(taskId, employeeCode);
      toast({
        title: "Task Completed",
        description: "Task has been marked as completed successfully.",
      });
      // OPTIMIZED: Update local state instead of full reload
      setTeamStats(prev => prev.map(team => ({
        ...team,
        employees: team.employees.map(emp => ({
          ...emp,
          tasks: emp.tasks.map(task => 
            task.task_id === taskId 
              ? { ...task, status: 'COMPLETED' }
              : task
          )
        }))
      })));
    } catch (error) {
      toast({
        title: "Error",
        description: "Failed to complete task. Please try again.",
        variant: "destructive",
      });
    }
  };

  const getTaskLoadColor = (taskCount: number) => {
    if (taskCount === 0) return "text-green-600 bg-green-50";
    if (taskCount <= 2) return "text-blue-600 bg-blue-50";
    if (taskCount <= 5) return "text-yellow-600 bg-yellow-50";
    return "text-red-600 bg-red-50";
  };

  const getTaskLoadText = (taskCount: number) => {
    if (taskCount === 0) return "Available";
    if (taskCount <= 2) return "Light Load";
    if (taskCount <= 5) return "Moderate Load";
    return "Heavy Load";
  };

  const toggleTeamExpansion = (teamCode: string) => {
    setExpandedTeams(prev => {
      const newSet = new Set(prev);
      if (newSet.has(teamCode)) {
        newSet.delete(teamCode);
      } else {
        newSet.add(teamCode);
      }
      return newSet;
    });
  };

  const toggleFileExpansion = (fileId: string) => {
    setExpandedFiles(prev => {
      const newSet = new Set(prev);
      if (newSet.has(fileId)) {
        newSet.delete(fileId);
      } else {
        newSet.add(fileId);
      }
      return newSet;
    });
  };

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'COMPLETED': return 'bg-success/20 text-success';
      case 'IN_PROGRESS': return 'bg-warning/20 text-warning';
      case 'ASSIGNED': return 'bg-secondary/20 text-secondary';
      default: return 'bg-muted/20 text-muted-foreground';
    }
  };

  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'COMPLETED': return <CheckCircle className="h-4 w-4" />;
      case 'IN_PROGRESS': return <Clock className="h-4 w-4" />;
      case 'ASSIGNED': return <AlertCircle className="h-4 w-4" />;
      default: return <AlertCircle className="h-4 w-4" />;
    }
  };

  return (
    <div className="space-y-6">
      {/* Page Header */}
      <div className="flex flex-col md:flex-row md:items-center md:justify-between gap-4">
        <div>
          <h1 className="text-3xl font-bold tracking-tight flex items-center gap-3">
            <KanbanSquare className="h-8 w-8 text-primary" />
            Team Lead Task Board
          </h1>
          <p className="text-muted-foreground mt-1">
            Active Teams: <span className="font-mono text-foreground">{teamStats?.length || 0}</span> · 
            Permit Files: <span className="font-mono text-foreground">{permitFiles?.length || 0}</span> · 
            Last updated: <span className="font-mono text-foreground">{lastRefresh.toLocaleTimeString()}</span> · 
            Auto-refreshes every 30s
          </p>
        </div>
        <div className="flex gap-2">
          <Button onClick={() => loadTeamLeadData()} variant="outline" size="sm">
            <RefreshCw className={`h-4 w-4 mr-2 ${isLoading ? 'animate-spin' : ''}`} />
            Refresh Now
          </Button>
        </div>
      </div>

      {/* Main Content */}
      {isLoading ? (
        <div className="flex items-center justify-center py-12">
          <Loader2 className="h-8 w-8 animate-spin text-primary" />
        </div>
      ) : (
        <Tabs defaultValue="teams" className="space-y-4">
          <TabsList className="grid w-full grid-cols-2">
            <TabsTrigger value="teams" className="flex items-center gap-2">
              <Users className="h-4 w-4" />
              Team Lead View
            </TabsTrigger>
            <TabsTrigger value="permit-files" className="flex items-center gap-2">
              <FileText className="h-4 w-4" />
              Permit File Tracking
            </TabsTrigger>
          </TabsList>

          {/* Team Lead View */}
          <TabsContent value="teams" className="space-y-4">
            {(!teamStats || teamStats.length === 0) ? (
              <Card>
                <CardContent className="text-center py-12">
                  <Users className="h-12 w-12 mx-auto text-muted-foreground mb-4" />
                  <p className="text-muted-foreground">
                    No team data available. Tasks may not be assigned yet.
                  </p>
                </CardContent>
              </Card>
            ) : (
              teamStats?.map((team) => (
                <Card key={team.team_lead_code}>
                  <Collapsible
                    open={expandedTeams.has(team.team_lead_code)}
                    onOpenChange={() => toggleTeamExpansion(team.team_lead_code)}
                  >
                    <CollapsibleTrigger asChild>
                      <CardHeader className="cursor-pointer hover:bg-muted/50 transition-colors">
                        <div className="flex items-center justify-between">
                          <div className="flex items-center gap-3">
                            <Users className="h-5 w-5 text-primary" />
                            <div>
                              <CardTitle className="text-lg">{team.team_lead_name}</CardTitle>
                              <p className="text-sm text-muted-foreground">
                                Team Lead • {team.unique_employees} employees working
                              </p>
                            </div>
                          </div>
                          <div className="flex items-center gap-4">
                            <div className="flex gap-4 text-sm">
                              <div className="text-center">
                                <p className="font-bold text-lg">{team.total_tasks}</p>
                                <p className="text-muted-foreground">Total</p>
                              </div>
                              <div className="text-center">
                                <p className="font-bold text-lg text-success">{team.completed_tasks}</p>
                                <p className="text-muted-foreground">Done</p>
                              </div>
                              <div className="text-center">
                                <p className="font-bold text-lg text-warning">{team.in_progress_tasks}</p>
                                <p className="text-muted-foreground">In Progress</p>
                              </div>
                              <div className="text-center">
                                <p className="font-bold text-lg text-secondary">{team.assigned_tasks}</p>
                                <p className="text-muted-foreground">Assigned</p>
                              </div>
                              <div className="text-center">
                                <p className="font-bold text-lg text-primary">{team.completion_rate}%</p>
                                <p className="text-muted-foreground">Complete</p>
                              </div>
                            </div>
                            {expandedTeams.has(team.team_lead_code) ? (
                              <ChevronUp key="up" className="h-4 w-4" />
                            ) : (
                              <ChevronDown key="down" className="h-4 w-4" />
                            )}
                          </div>
                        </div>
                      </CardHeader>
                    </CollapsibleTrigger>
                    <CollapsibleContent>
                      <CardContent className="space-y-3">
                        {team.employees.map((employee) => (
                          <div key={employee.employee_code} className="border rounded-lg">
                            {/* Employee Header with Task Count */}
                            <div className="flex items-center justify-between p-3 bg-muted/30">
                              <div className="flex items-center gap-3">
                                <div>
                                  <p className="font-medium">{employee.employee_name}</p>
                                  <p className="text-sm text-muted-foreground">{employee.employee_role}</p>
                                </div>
                                <Badge className={getTaskLoadColor(employee.task_count)}>
                                  {employee.task_count} tasks - {getTaskLoadText(employee.task_count)}
                                </Badge>
                              </div>
                              <div className="flex items-center gap-2">
                                <Button 
                                  variant="ghost" 
                                  size="sm"
                                  onClick={() => navigate(`/employees/${employee.employee_code}`, { state: { from: 'task-board' } })}
                                >
                                  View Profile
                                </Button>
                              </div>
                            </div>
                            
                            {/* Tasks List - Expandable */}
                            {employee.tasks.length > 0 && (
                              <div className="border-t">
                                <div className="p-3 space-y-2">
                                  {employee.tasks.map((task) => (
                                    <div key={task.task_id} className="flex items-center justify-between p-2 bg-background rounded border">
                                      <div className="flex-1">
                                        <p className="text-sm font-medium">{task.task_title}</p>
                                        <p className="text-xs text-muted-foreground">
                                          Assigned: {formatDate(task.assigned_at)}
                                          {task.completed_at && ` • Completed: ${formatDate(task.completed_at)}`}
                                        </p>
                                      </div>
                                      <div className="flex items-center gap-2">
                                        <Badge className={getStatusColor(task.status)}>
                                          {getStatusIcon(task.status)}
                                          <span className="ml-1">{task.status}</span>
                                        </Badge>
                                        {task.status === 'ASSIGNED' && (
                                          <Button 
                                            variant="ghost" 
                                            size="sm"
                                            onClick={() => handleCompleteTask(task.task_id, employee.employee_code)}
                                            className="text-green-600 hover:text-green-700 hover:bg-green-50"
                                          >
                                            <CheckCircle className="h-4 w-4" />
                                          </Button>
                                        )}
                                      </div>
                                    </div>
                                  ))}
                                </div>
                              </div>
                            )}
                          </div>
                        ))}
                      </CardContent>
                    </CollapsibleContent>
                  </Collapsible>
                </Card>
              ))
            )}
          </TabsContent>

          {/* Permit File Tracking */}
          <TabsContent value="permit-files" className="space-y-4">
            {(!permitFiles || permitFiles.length === 0) ? (
              <Card>
                <CardContent className="text-center py-12">
                  <FileText className="h-12 w-12 mx-auto text-muted-foreground mb-4" />
                  <p className="text-muted-foreground">
                    No permit files with assigned tasks found.
                  </p>
                </CardContent>
              </Card>
            ) : (
              permitFiles?.map((permitFile) => {
                const fileId = getFileId(permitFile) || '';
                const uniqueKey = `permit-${fileId}-teamlead`;
                
                // Debug logging to track key usage
                if (process.env.NODE_ENV === 'development') {
                  console.log(`TeamLeadTaskBoard rendering key: ${uniqueKey} for file: ${fileId}`);
                }
                
                return (
                  <Card key={uniqueKey}>
                  <Collapsible
                    open={expandedFiles.has(fileId)}
                    onOpenChange={() => toggleFileExpansion(fileId)}
                  >
                    <CollapsibleTrigger asChild>
                      <CardHeader className="cursor-pointer hover:bg-muted/50 transition-colors">
                        <div className="flex items-center justify-between">
                          <div className="flex items-center gap-3">
                            <FileText className="h-5 w-5 text-primary" />
                            <div>
                              <CardTitle className="text-lg">Permit File: {getFileDisplayName(permitFile)}</CardTitle>
                              <p className="text-sm text-muted-foreground">
                                Status: {permitFile.status}
                              </p>
                            </div>
                          </div>
                          <div className="flex items-center gap-4">
                            <div className="flex gap-4 text-sm">
                              <div className="text-center">
                                <p className="font-bold text-lg">{permitFile.total_tasks}</p>
                                <p className="text-muted-foreground">Total</p>
                              </div>
                              <div className="text-center">
                                <p className="font-bold text-lg text-success">{permitFile.completed_tasks}</p>
                                <p className="text-muted-foreground">Done</p>
                              </div>
                              <div className="text-center">
                                <p className="font-bold text-lg text-warning">{permitFile.in_progress_tasks}</p>
                                <p className="text-muted-foreground">In Progress</p>
                              </div>
                              <div className="text-center">
                                <p className="font-bold text-lg text-secondary">{permitFile.assigned_tasks}</p>
                                <p className="text-muted-foreground">Assigned</p>
                              </div>
                              <div className="text-center">
                                <p className="font-bold text-lg text-primary">{permitFile.completion_rate}%</p>
                                <p className="text-muted-foreground">Complete</p>
                              </div>
                            </div>
                            <Badge className={getStatusColor(permitFile.status)}>
                              {getStatusIcon(permitFile.status)}
                              <span className="ml-1">{permitFile.status}</span>
                            </Badge>
                            {expandedFiles.has(fileId) ? (
                              <ChevronUp key="up" className="h-4 w-4" />
                            ) : (
                              <ChevronDown key="down" className="h-4 w-4" />
                            )}
                          </div>
                        </div>
                      </CardHeader>
                    </CollapsibleTrigger>
                    <CollapsibleContent>
                      <CardContent className="space-y-3">
                        {permitFile.tasks.map((task) => (
                          <div key={task.task_id} className="flex items-center justify-between p-3 rounded-lg border">
                              <div className="flex-1">
                                <div className="flex items-center gap-2 mb-1">
                                  <p className="font-medium">{task.employee_name}</p>
                                {task.team_lead && (
                                  <Badge variant="outline" className="text-xs">
                                    Team: {task.team_lead}
                                  </Badge>
                                )}
                              </div>
                              <p className="text-sm text-muted-foreground">
                                {task.task_title}
                              </p>
                              <p className="text-xs text-muted-foreground mt-1">
                                Assigned: {formatDate(task.assigned_at)}
                                {task.completed_at && ` • Completed: ${formatDate(task.completed_at)}`}
                              </p>
                            </div>
                            <div className="flex items-center gap-2">
                              <Badge className={getStatusColor(task.status)}>
                                {getStatusIcon(task.status)}
                                <span className="ml-1">{task.status}</span>
                              </Badge>
                              {task.status === 'ASSIGNED' && (
                                <Button 
                                  variant="ghost" 
                                  size="sm"
                                  onClick={() => handleCompleteTask(task.task_id, task.assigned_to)}
                                  className="text-green-600 hover:text-green-700 hover:bg-green-50"
                                >
                                  <CheckCircle className="h-4 w-4" />
                                </Button>
                              )}
                              <Button 
                                variant="ghost" 
                                size="sm"
                                onClick={() => navigate(`/employees/${task.assigned_to}`, { state: { from: 'task-board' } })}
                              >
                                View
                              </Button>
                            </div>
                          </div>
                        ))}
                      </CardContent>
                    </CollapsibleContent>
                  </Collapsible>
                </Card>
                );
              })
            )}
          </TabsContent>
        </Tabs>
      )}
    </div>
  );
}

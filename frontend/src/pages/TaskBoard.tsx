import { KanbanSquare, Plus, MoreVertical, User, Clock, Loader2, CheckCircle, RefreshCw } from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";
import { getEmployees, Employee, Task, getEmployeeTasks, completeTask, getAllAssignedTasks, getEmployeeAssignedTasks } from "@/lib/api";
import { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import { useToast } from "@/hooks/use-toast";

// Helper function to format dates with proper timezone handling
const formatDate = (dateString: string | undefined | null): string => {
  if (!dateString) return 'No date';
  
  try {
    // Handle different date formats
    let date: Date;
    if (dateString.includes('T')) {
      // ISO format
      if (!dateString.endsWith('Z') && !dateString.includes('+')) {
        // Add Z if missing for UTC
        date = new Date(dateString + 'Z');
      } else {
        date = new Date(dateString);
      }
    } else {
      date = new Date(dateString);
    }
    
    if (isNaN(date.getTime())) {
      return 'Invalid date';
    }
    
    // Format with local timezone
    return date.toLocaleString('en-US', {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit'
    });
  } catch (error) {
    return 'Invalid date';
  }
};

interface TaskItem extends Task {
  employee_name?: string;
  employee_role?: string;
  title?: string;
  assignee?: string;
  timeEstimate?: number;
  id?: string; // Added id property
  // Ensure all Task properties are explicitly available
  employee_code: string;
  task_description: string;
  status: 'OPEN' | 'ASSIGNED' | 'IN_PROGRESS' | 'DONE' | 'COMPLETED';
  assigned_at: string;
  time_assigned: string;
  task_id: string;
}

interface Column {
  id: string;
  title: string;
  color: string;
  tasks: TaskItem[];
}

const statusColors = {
  OPEN: "bg-muted text-muted-foreground",
  ASSIGNED: "bg-primary/20 text-primary",
  IN_PROGRESS: "bg-warning/20 text-warning",
  DONE: "bg-success/20 text-success",
};

export default function TaskBoard() {
  const [assignedTasks, setAssignedTasks] = useState<TaskItem[]>([]);
  const [employees, setEmployees] = useState<Employee[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [lastRefresh, setLastRefresh] = useState<Date>(new Date());
  const [lastUpdateTime, setLastUpdateTime] = useState<string | null>(null);
  const navigate = useNavigate();
  const { toast } = useToast();

  useEffect(() => {
    loadAssignedTasks();
    
    // Set up polling for real-time updates (every 30 seconds)
    const interval = setInterval(() => {
      loadAssignedTasks(false); // Silent refresh without loading indicator
    }, 30000);

    const onTaskAssigned = () => {
      loadAssignedTasks(false);
    };

    window.addEventListener('task_assigned', onTaskAssigned as EventListener);
    
    return () => {
      clearInterval(interval);
      window.removeEventListener('task_assigned', onTaskAssigned as EventListener);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps -- preserve current behavior: only run on mount
  }, []);

  const handleCompleteTask = async (taskId: string, employeeCode: string) => {
    try {
      await completeTask(taskId, employeeCode);
      toast({
        title: "Task Completed",
        description: "Task has been marked as completed successfully.",
      });
      // OPTIMIZED: Update local state instead of reloading all tasks
      setAssignedTasks(prev => prev.map(task => 
        task.task_id === taskId 
          ? { ...task, status: 'COMPLETED' as const }
          : task
      ));
    } catch (error) {
      toast({
        title: "Error",
        description: "Failed to complete task. Please try again.",
        variant: "destructive",
      });
    }
  };

  const handleCompleteAndNavigate = async (taskId: string, employeeCode: string) => {
    try {
      await completeTask(taskId, employeeCode);
      toast({
        title: "Task Completed",
        description: "Task has been marked as completed successfully.",
      });
      // OPTIMIZED: Update local state instead of reloading all tasks
      setAssignedTasks(prev => prev.map(task => 
        task.task_id === taskId 
          ? { ...task, status: 'COMPLETED' as const }
          : task
      ));
    } catch (error) {
      toast({
        title: "Error",
        description: "Failed to complete task. Please try again.",
        variant: "destructive",
      });
    }
  };

  const loadAssignedTasks = async (showLoading = true) => {
    if (showLoading) setIsLoading(true);
    try {
      // OPTIMIZED: Single API call to get all assigned tasks with employee details
      const data = await getAllAssignedTasks();
      
      const tasksArray = Array.isArray(data) ? data : (data?.tasks || []);
      const total = Array.isArray(data) ? tasksArray.length : (data?.total ?? tasksArray.length);
      const lastUpdated = Array.isArray(data) ? null : (data?.last_updated ?? null);
      
      // Transform data to match existing TaskItem format
      const transformedTasks = tasksArray.map((task: unknown) => {
        const t = task as {
          employee_details?: { employee_name?: string; employment?: { current_role?: string }; employee_code?: string };
          assigned_to_name?: string;
          assigned_to?: string;
          employee_code?: string;
          title?: string;
          task_assigned?: string;
          task_description?: string;
          assigned_at?: string;
          time_assigned?: string;
        };
        return {
        ...(task as object),
        employee_name: t.employee_details?.employee_name || t.assigned_to_name,
        employee_role: t.employee_details?.employment?.current_role,
        employee_code: t.assigned_to || t.employee_details?.employee_code || t.employee_code,
        title: t.title || t.task_assigned || t.task_description,
        assignee: t.employee_details?.employee_name || t.assigned_to_name,
        id: `${t.assigned_to || t.employee_details?.employee_code || t.employee_code}-${t.assigned_at || t.time_assigned}`,
        timeEstimate: undefined,
      };
      });
      
      setAssignedTasks(transformedTasks as TaskItem[]);
      setLastUpdateTime(lastUpdated);
      setLastRefresh(new Date());
      
      console.log(`📊 Loaded ${total} assigned tasks in single API call`);
    } catch (error) {
      console.error('Error loading tasks:', error);
      // Fallback to old method if new endpoint fails
      await loadAssignedTasksFallback(showLoading);
    } finally {
      if (showLoading) setIsLoading(false);
    }
  };

  // Fallback method for compatibility
  const loadAssignedTasksFallback = async (showLoading = true) => {
    if (showLoading) setIsLoading(true);
    try {
      // Get employees data
      const employeesData = await getEmployees();
      setEmployees(employeesData);
      
      // Get assigned tasks for each employee - batch requests for better performance
      const taskPromises = employeesData.map(async (employee) => {
        try {
          const tasks = await getEmployeeTasks(employee.employee_code);
          return tasks.tasks.map(task => ({
            ...task,
            employee_name: employee.employee_name,
            employee_role: employee.current_role,
            employee_code: employee.employee_code,
            title: task.task_assigned || task.task_description,
            assignee: employee.employee_name,
            timeEstimate: undefined,
            id: `${employee.employee_code}-${task.time_assigned || task.assigned_at}`,
          }));
        } catch (error) {
          // Skip if employee has no tasks
          return [];
        }
      });
      
      const allTasks = (await Promise.all(taskPromises)).flat();
      setAssignedTasks(allTasks);
      setLastRefresh(new Date());
    } catch (error) {
      console.error('Error loading tasks:', error);
    } finally {
      if (showLoading) setIsLoading(false);
    }
  };

  // Incremental update for single employee
  const updateEmployeeTasks = async (employeeCode: string) => {
    try {
      console.log(`🔄 Updating tasks for employee: ${employeeCode}`);
      
      const data = await getEmployeeAssignedTasks(employeeCode);
      
      // Remove existing tasks for this employee
      const filteredTasks = assignedTasks.filter((task: TaskItem) => task.employee_code !== employeeCode);
      
      // API now returns array directly, check if data has tasks property or is array
      const tasksArray = Array.isArray(data) ? data : ((data as { tasks?: unknown[] })?.tasks || []);
      
      // Get employee info from the response if available, otherwise use fallback
      const employeeInfo = (data as { employee?: { employee_name?: string; employment?: { current_role?: string }; employee_code?: string } })?.employee;
      const employeeName = employeeInfo?.employee_name || 'Unknown';
      const employeeRole = employeeInfo?.employment?.current_role || '';
      const employeeCodeFromResponse = employeeInfo?.employee_code || employeeCode;
      
      // Add updated tasks
      const newTasks = tasksArray.map((task: unknown) => {
        const t = task as { title?: string; task_assigned?: string; task_description?: string; assigned_at?: string; time_assigned?: string };
        return {
        ...(task as object),
        employee_name: employeeName,
        employee_role: employeeRole,
        employee_code: employeeCodeFromResponse,
        title: t.title || t.task_assigned || t.task_description,
        assignee: employeeName,
        id: `${employeeCodeFromResponse}-${t.assigned_at || t.time_assigned}`,
        timeEstimate: undefined,
      };
      });
      
      setAssignedTasks([...filteredTasks, ...newTasks] as TaskItem[]);
      setLastRefresh(new Date());
      
      console.log(`✅ Updated ${tasksArray.length} tasks for ${employeeName}`);
    } catch (error) {
      console.error('Error updating employee tasks:', error);
    }
  };

  return (
    <div className="space-y-6">
      {/* Page Header */}
      <div className="flex flex-col md:flex-row md:items-center md:justify-between gap-4">
        <div>
          <h1 className="text-3xl font-bold tracking-tight flex items-center gap-3">
            <KanbanSquare className="h-8 w-8 text-primary" />
            Task Board
          </h1>
          <p className="text-muted-foreground mt-1">
            Active Tasks: <span className="font-mono text-foreground">{assignedTasks.filter(t => t.status !== 'COMPLETED').length}</span> · 
            Last updated: <span className="font-mono text-foreground">{lastRefresh.toLocaleTimeString()}</span> · 
            Auto-refreshes every 30s
          </p>
        </div>
        <div className="flex gap-2">
          <Button onClick={() => loadAssignedTasks()} variant="outline" size="sm">
            <RefreshCw className="h-4 w-4 mr-2" />
            Refresh Now
          </Button>
        </div>
      </div>

      {/* Kanban Board */}
      {isLoading ? (
        <div className="flex items-center justify-center py-12">
          <Loader2 className="h-8 w-8 animate-spin text-primary" />
        </div>
      ) : (
        <div className="grid grid-cols-1 gap-6 min-h-[600px]">
          <Card className="bg-secondary/30 border-border">
            <CardHeader className="pb-3">
              <CardTitle className="flex items-center justify-between">
                <div className="flex items-center gap-2">
                  <div className="w-3 h-3 rounded-full bg-primary" />
                  <span className="text-base">Assigned Tasks</span>
                  <Badge variant="muted" className="ml-1">
                    {assignedTasks.length}
                  </Badge>
                </div>
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-3 min-h-[400px]">
              {assignedTasks.length > 0 ? (
                assignedTasks.map((task: TaskItem, index) => (
                  <div 
                    key={`${task.employee_code || 'unknown'}-${task.assigned_at || task.time_assigned || index}-${index}`}
                    className="p-3 rounded-lg bg-card border border-border hover:border-primary/30 transition-all animate-scale-in group"
                    style={{ animationDelay: `${index * 50}ms` }}
                  >
                    <div className="flex items-start justify-between gap-2">
                      <div className="flex-1">
                        <h4 className="text-sm font-medium leading-tight">{task.title || task.task_description || 'Untitled Task'}</h4>
                        <div className="mt-2 flex items-center gap-2 text-xs text-muted-foreground">
                          <div className="w-5 h-5 rounded-full bg-primary/20 flex items-center justify-center">
                            <User className="h-3 w-3 text-primary" />
                          </div>
                          <span className="truncate">{task.employee_name}</span>
                          <Badge variant="outline" className="text-xs">
                            #{task.employee_code}
                          </Badge>
                        </div>
                        <div className="mt-2 flex items-center gap-2 text-xs text-muted-foreground">
                          <Clock className="h-3 w-3" />
                          <span>
                            {formatDate(task.assigned_at || task.time_assigned)}
                          </span>
                          <Badge variant={task.status === 'ASSIGNED' ? 'secondary' : 'default'} className="text-xs">
                            {task.status}
                          </Badge>
                        </div>
                      </div>
                      <div className="flex gap-1">
                        <Button 
                          variant="ghost" 
                          size="icon-sm" 
                          onClick={() => {
                            const employeeCode = task.employee_code;
                            if (!employeeCode || employeeCode === 'null' || employeeCode === 'undefined') {
                              toast({
                                title: 'Error',
                                description: 'Employee code is missing for this task',
                                variant: 'destructive',
                              });
                              return;
                            }
                            navigate(`/employees/${employeeCode}`, { state: { from: 'task-board' } });
                          }}
                        >
                          <User className="h-3 w-3" />
                        </Button>
                        {task.status === 'ASSIGNED' && task.task_id && (
                          <Button 
                            variant="ghost" 
                            size="icon-sm" 
                            onClick={() => handleCompleteAndNavigate(task.task_id, task.employee_code)}
                            className="text-green-600 hover:text-green-700 hover:bg-green-50"
                            title="Complete task"
                          >
                            <CheckCircle className="h-3 w-3" />
                          </Button>
                        )}
                      </div>
                    </div>
                  </div>
                ))
              ) : (
                <div className="text-center py-12">
                  <p className="text-muted-foreground">
                    No tasks assigned yet. Go to the Smart Recommender to assign tasks.
                  </p>
                  <Button 
                    className="mt-4" 
                    onClick={() => navigate('/recommender')}
                  >
                    Assign Tasks
                  </Button>
                </div>
              )}
            </CardContent>
          </Card>
        </div>
      )}
    </div>
  );
}

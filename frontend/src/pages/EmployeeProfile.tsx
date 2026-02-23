import { useParams, Link, useLocation, useNavigate } from "react-router-dom";
import { useState, useEffect } from "react";
import { ArrowLeft, User, Mail, Clock, Briefcase, Calendar, Star, CheckCircle2, BarChart3, TrendingUp, RefreshCw, Edit, Timer, AlertTriangle, Target, Activity } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Skeleton } from "@/components/ui/skeleton";
import { Progress } from "@/components/ui/progress";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { getEmployee, getEmployeeTasks, getEmployeeTaskStats, getEmployeeCompletedTasks } from "@/lib/api";
import { useToast } from "@/hooks/use-toast";
import { Employee, Task } from "@/types";

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

// Helper function to calculate duration between two dates
const calculateDuration = (startTime: string, endTime: string): string => {
  if (!startTime || !endTime) return 'N/A';
  
  try {
    const start = new Date(startTime);
    const end = new Date(endTime);
    
    if (isNaN(start.getTime()) || isNaN(end.getTime())) {
      return 'Invalid dates';
    }
    
    const durationMs = end.getTime() - start.getTime();
    const durationHours = Math.floor(durationMs / (1000 * 60 * 60));
    const durationMinutes = Math.floor((durationMs % (1000 * 60 * 60)) / (1000 * 60));
    
    if (durationHours > 0) {
      return `${durationHours}h ${durationMinutes}m`;
    } else {
      return `${durationMinutes}m`;
    }
  } catch (error) {
    return 'Error';
  }
};

// Helper function to calculate duration in minutes
const calculateDurationMinutes = (startTime: string, endTime: string): number => {
  if (!startTime || !endTime) return 0;
  
  try {
    const start = new Date(startTime);
    const end = new Date(endTime);
    
    if (isNaN(start.getTime()) || isNaN(end.getTime())) {
      return 0;
    }
    
    return Math.floor((end.getTime() - start.getTime()) / (1000 * 60));
  } catch (error) {
    return 0;
  }
};

// Stage configuration with time standards (from backend)
const STAGE_TIME_STANDARDS = {
  'PRELIMS': { ideal: 20, max: 30, display: 'Prelims' },
  'PRODUCTION': { ideal: 210, max: 240, display: 'Production' },
  'COMPLETED': { ideal: 0, max: 5, display: 'Completed' },
  'QC': { ideal: 90, max: 120, display: 'Quality Control' },
  'DELIVERED': { ideal: 0, max: 5, display: 'Delivered' }
};

// Helper function to get performance color based on timing
const getTimingPerformanceColor = (actualMinutes: number, idealMinutes: number, maxMinutes: number): { color: string, status: string, variant: 'default' | 'secondary' | 'destructive' | 'outline' } => {
  if (actualMinutes <= idealMinutes) {
    return { color: 'text-green-600', status: 'Excellent', variant: 'default' as const };
  } else if (actualMinutes <= maxMinutes) {
    return { color: 'text-yellow-600', status: 'Good', variant: 'secondary' as const };
  } else {
    return { color: 'text-red-600', status: 'Overdue', variant: 'destructive' as const };
  }
};

export default function EmployeeProfile() {
  const { id } = useParams();
  const navigate = useNavigate();
  const location = useLocation();
  const [employee, setEmployee] = useState<Employee | null>(null);
  const [assignedTasks, setAssignedTasks] = useState<Task[]>([]);
  const [completedTasks, setCompletedTasks] = useState<Task[]>([]);
  const [taskStats, setTaskStats] = useState<any>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  
  // Skill modal state
  const [selectedSkill, setSelectedSkill] = useState<string>("");
  const [showSkillModal, setShowSkillModal] = useState(false);
  const [employeesWithSkill, setEmployeesWithSkill] = useState<Employee[]>([]);
  
  // Check where we came from
  const cameFromRecommender = location.state?.from === 'smart-recommender';
  const cameFromTaskBoard = location.state?.from === 'task-board';
  const taskCompleted = location.state?.taskCompleted; // Check if a task was just completed
  
  useEffect(() => {
    if (id && id !== 'null' && id !== 'undefined') {
      console.log('🔍 Loading employee profile for ID:', id);
      // Load all data in parallel for better performance
      const loadAllData = async () => {
        if (!id || id === 'null' || id === 'undefined') {
          console.error('❌ No employee ID provided');
          setError('No employee ID provided');
          setIsLoading(false);
          return;
        }
        
        setIsLoading(true);
        try {
          console.log('🔍 Loading data for employee ID:', id);
          const [employeeData, tasksData, statsData, completedData] = await Promise.allSettled([
            getEmployee(id),
            getEmployeeTasks(id),
            getEmployeeTaskStats(id),
            getEmployeeCompletedTasks(id)
          ]);
          
          if (employeeData.status === 'fulfilled') {
            console.log('✅ Employee data loaded:', employeeData.value);
            setEmployee(employeeData.value);
          } else {
            console.error('❌ Failed to load employee:', employeeData.reason);
            const errorMsg = employeeData.reason?.toString() || '';
            if (errorMsg.includes('not found') || errorMsg.includes('404')) {
              setError(`Employee ${id} not found. Please check the employee code and try again.`);
              // Optionally redirect to a valid employee after 3 seconds
              setTimeout(() => {
                console.log('Redirecting to a valid employee...');
                navigate('/employees/1030'); // Redirect to a known valid employee
              }, 3000);
            } else {
              setError(`Failed to load employee: ${employeeData.reason}`);
            }
          }
          
          if (tasksData.status === 'fulfilled') {
            const td = tasksData.value as any;
            setAssignedTasks(td.assigned_tasks || td.tasks || []);
          } else {
            console.error('❌ Failed to load tasks:', tasksData.reason);
          }
          
          if (statsData.status === 'fulfilled') {
            setTaskStats(statsData.value);
          } else {
            console.error('❌ Failed to load stats:', statsData.reason);
          }
          
          if (completedData.status === 'fulfilled') {
            const cd = completedData.value as any;
            setCompletedTasks(cd.completed_tasks || cd.tasks || []);
          } else {
            console.error('❌ Failed to load completed tasks:', completedData.reason);
          }
        } catch (error) {
          console.error('❌ Error loading employee data:', error);
          setError('Failed to load employee data. Please try again.');
        } finally {
          setIsLoading(false);
        }
      };
      
      loadAllData();
    } else {
      console.error('❌ No employee ID provided in URL');
      setError('No employee ID provided. Please navigate from the employee directory.');
      setIsLoading(false);
    }
  }, [id]);

  // Refresh data when returning from task board after completing a task
  useEffect(() => {
    if (taskCompleted && id) {
      console.log('Task completed detected, refreshing employee data...');
      refreshData();
    }
  }, [taskCompleted, id]);

  const refreshData = async () => {
    if (id) {
      setIsLoading(true);
      try {
        const [employeeData, tasksData, statsData, completedData] = await Promise.allSettled([
          getEmployee(id),
          getEmployeeTasks(id),
          getEmployeeTaskStats(id),
          getEmployeeCompletedTasks(id)
        ]);
        
        if (employeeData.status === 'fulfilled') {
          setEmployee(employeeData.value);
        }
        
        if (tasksData.status === 'fulfilled') {
          const td = tasksData.value as any;
          setAssignedTasks(td.assigned_tasks || td.tasks || []);
        }
        
        if (statsData.status === 'fulfilled') {
          console.log('Task stats loaded:', statsData.value);
          setTaskStats(statsData.value);
        } else {
          console.error('Task stats not fulfilled:', statsData.status);
        }
        
        if (completedData.status === 'fulfilled') {
          const cd = completedData.value as any;
          setCompletedTasks(cd.completed_tasks || cd.tasks || []);
        }
      } catch (error) {
        console.error('Error refreshing data:', error);
      } finally {
        setIsLoading(false);
      }
    }
  };

  // Handle skill click - similar to SmartRecommender
  const handleSkillClick = async (skill: string) => {
    console.log(`🎯 Skill clicked: ${skill}`);
    setSelectedSkill(skill);
    
    // For now, just show the modal with test data that matches Employee interface
    const testEmployees: Employee[] = [
      {
        employee_code: "1030",
        employee_name: "Md Monazir Hasan",
        technical_skills: {
          structural_design: [skill],
          electrical_design: [],
          coordination: []
        },
        current_role: "Structural Engineer"
      },
      {
        employee_code: "1001", 
        employee_name: "Test Employee",
        technical_skills: {
          structural_design: [],
          electrical_design: [skill],
          coordination: []
        },
        current_role: "Electrical Engineer"
      }
    ];
    
    setEmployeesWithSkill(testEmployees);
    setShowSkillModal(true);
    console.log(`👥 Showing modal with ${testEmployees.length} test employees for skill: ${skill}`);
  };

  // Extract skills function - same as SmartRecommender
  const extractSkills = (employee: Employee): string[] => {
    if (employee.technical_skills) {
      return [
        ...(employee.technical_skills.structural_design || []),
        ...(employee.technical_skills.electrical_design || []),
        ...(employee.technical_skills.coordination || [])
      ];
    }
    return employee.skills || [];
  };

  if (isLoading) {
    return (
      <div className="space-y-6 max-w-4xl mx-auto">
        <Skeleton className="h-10 w-32" />
        <Card>
          <CardContent className="pt-6">
            <div className="flex items-start gap-6">
              <Skeleton className="w-24 h-24 rounded-2xl" />
              <div className="flex-1 space-y-4">
                <Skeleton className="h-8 w-64" />
                <Skeleton className="h-4 w-48" />
                <div className="grid grid-cols-3 gap-4">
                  <Skeleton className="h-4 w-full" />
                  <Skeleton className="h-4 w-full" />
                  <Skeleton className="h-4 w-full" />
                </div>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>
    );
  }

  if (error || !employee) {
    return (
      <div className="flex flex-col items-center justify-center min-h-[400px] gap-4 p-8">
        <div className="text-center">
          <AlertTriangle className="h-12 w-12 text-red-500 mx-auto mb-4" />
          <h2 className="text-xl font-semibold text-gray-900 mb-2">
            Employee Not Found
          </h2>
          <p className="text-lg text-muted-foreground mb-4">
            {error || 'Employee not found'}
          </p>
          {error?.includes('not found') && (
            <div className="bg-blue-50 border border-blue-200 rounded-lg p-4 mb-4">
              <p className="text-sm text-blue-800">
                <strong>Valid employee codes:</strong> 1030, 878, 958, 30, 652, etc.
              </p>
              <p className="text-sm text-blue-600 mt-1">
                You will be redirected to employee 1030 automatically in 3 seconds...
              </p>
            </div>
          )}
        </div>
        <div className="flex gap-2">
          <Button onClick={() => navigate('/employees')} variant="outline">
            <User className="h-4 w-4 mr-2" />
            Employee Directory
          </Button>
          <Button onClick={() => navigate(-1)}>
            <ArrowLeft className="h-4 w-4 mr-2" />
            Back to Previous Page
          </Button>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6 max-w-4xl mx-auto">
      {/* Back and Refresh Buttons */}
      <div className="flex items-center justify-between">
        <Button variant="ghost" onClick={() => {
          if (cameFromRecommender) {
            navigate(-1); // Go back to Smart Recommender
          } else if (cameFromTaskBoard) {
            navigate('/task-board'); // Go back to TaskBoard
          } else {
            navigate('/employees'); // Go to employees directory
          }
        }}>
          <ArrowLeft className="h-4 w-4" />
          {cameFromRecommender ? 'Back to Recommendations' : cameFromTaskBoard ? 'Back to Task Board' : 'Back to Directory'}
        </Button>
        <div className="flex gap-2">
          <Button variant="outline" onClick={refreshData} disabled={isLoading}>
            <RefreshCw className={`h-4 w-4 mr-2 ${isLoading ? 'animate-spin' : ''}`} />
            Refresh
          </Button>
          <Button 
            variant="default" 
            onClick={() => navigate(`/employees/${id}/edit`)}
            className="flex items-center gap-2"
          >
            <Edit className="h-4 w-4" />
            Edit Profile
          </Button>
        </div>
      </div>

      {/* Profile Header */}
      <Card variant="glow">
        <CardContent className="pt-6">
          <div className="flex flex-col md:flex-row items-start gap-6">
            {/* Avatar */}
            <div className="w-24 h-24 rounded-2xl bg-primary/20 flex items-center justify-center">
              <User className="h-12 w-12 text-primary" />
            </div>
            
            {/* Info */}
            <div className="flex-1">
              <div className="flex items-start justify-between">
                <div>
                  <h1 className="text-2xl font-bold">
                    {employee.employee_name}
                  </h1>
                  <p className="text-muted-foreground flex items-center gap-2 mt-1">
                    <Briefcase className="h-4 w-4" />
                    {employee.current_role}
                    <span className="text-border">•</span>
                    <span className="font-mono text-sm">#{employee.employee_code}</span>
                  </p>
                </div>
                <Badge variant="success">Available</Badge>
              </div>
              
              <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mt-6">
                {employee.contact_email && (
                  <div className="flex items-center gap-2 text-sm">
                    <Mail className="h-4 w-4 text-muted-foreground" />
                    <span>{employee.contact_email}</span>
                  </div>
                )}
                {employee.current_experience_years && (
                  <div className="flex items-center gap-2 text-sm">
                    <Clock className="h-4 w-4 text-muted-foreground" />
                    <span>{employee.current_experience_years.toFixed(1)} years experience</span>
                  </div>
                )}
                {employee.shift && (
                  <div className="flex items-center gap-2 text-sm">
                    <Calendar className="h-4 w-4 text-muted-foreground" />
                    <span>{employee.shift} Shift</span>
                  </div>
                )}
              </div>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Tabs */}
      <Tabs defaultValue="skills" className="space-y-4">
        <TabsList>
          <TabsTrigger value="skills">Skills</TabsTrigger>
          <TabsTrigger value="tasks">Recent Tasks</TabsTrigger>
          <TabsTrigger value="performance">Performance</TabsTrigger>
        </TabsList>
        
        <TabsContent value="skills" className="space-y-4">
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <Star className="h-5 w-5 text-primary" />
                Technical Skills
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="space-y-4">
                {(() => {
                  // Get skills from either field with priority to 'skills'
                  const getSkills = () => {
                    const skills = {
                      structural_design: [],
                      electrical_design: [],
                      coordination: []
                    };
                    
                    console.log('🔍 Checking skills for employee:', employee.employee_code);
                    console.log('  employee.skills:', employee.skills);
                    console.log('  employee.technical_skills:', employee.technical_skills);
                    
                    // Try technical_skills field first (primary location from database)
                    if (employee.technical_skills?.structural_design) skills.structural_design = employee.technical_skills.structural_design;
                    if (employee.technical_skills?.electrical_design) skills.electrical_design = employee.technical_skills.electrical_design;
                    if (employee.technical_skills?.coordination) skills.coordination = employee.technical_skills.coordination;
                    
                    // Fallback to skills if technical_skills is empty
                    if (skills.structural_design.length === 0 && skills.electrical_design.length === 0 && skills.coordination.length === 0) {
                      // If skills is a simple array, use it as structural_design by default
                      if (employee.skills && Array.isArray(employee.skills) && employee.skills.length > 0) {
                        skills.structural_design = employee.skills;
                      } else {
                        // Add test skills for demonstration
                        skills.structural_design = ["structural_analysis", "steel_design"];
                        skills.electrical_design = ["circuit_design", "power_systems"];
                        skills.coordination = ["project_management"];
                      }
                    }
                    
                    console.log('  Final skills:', skills);
                    return skills;
                  };
                  
                  const skills = getSkills();
                  
                  return (
                    <>
                      {/* Structural Design Skills */}
                      {skills.structural_design && skills.structural_design.length > 0 && (
                        <div>
                          <p className="text-sm font-medium text-muted-foreground mb-2">Structural Design:</p>
                          <div className="flex flex-wrap gap-2">
                            {skills.structural_design.map((skill) => (
                              <Badge 
                                key={skill} 
                                variant="secondary" 
                                className="px-3 py-1.5 hover:bg-blue-200 transition-colors"
                                style={{ cursor: 'pointer' }}
                                onClick={() => {
                                  console.log('🎯 Skill clicked:', skill);
                                  handleSkillClick(skill);
                                }}
                              >
                                {skill}
                              </Badge>
                            ))}
                          </div>
                        </div>
                      )}
                      
                      {/* Electrical Design Skills */}
                      {skills.electrical_design && skills.electrical_design.length > 0 && (
                        <div>
                          <p className="text-sm font-medium text-muted-foreground mb-2">Electrical Design:</p>
                          <div className="flex flex-wrap gap-2">
                            {skills.electrical_design.map((skill) => (
                              <Badge 
                                key={skill} 
                                variant="secondary" 
                                className="px-3 py-1.5 bg-green-100 text-green-800 hover:bg-green-200 transition-colors"
                                style={{ cursor: 'pointer' }}
                                onClick={() => {
                                  console.log('🎯 Skill clicked:', skill);
                                  handleSkillClick(skill);
                                }}
                              >
                                {skill}
                              </Badge>
                            ))}
                          </div>
                        </div>
                      )}
                      
                      {/* Coordination Skills */}
                      {skills.coordination && skills.coordination.length > 0 && (
                        <div>
                          <p className="text-sm font-medium text-muted-foreground mb-2">Coordination:</p>
                          <div className="flex flex-wrap gap-2">
                            {skills.coordination.map((skill) => (
                              <Badge 
                                key={skill} 
                                variant="secondary" 
                                className="px-3 py-1.5 bg-purple-100 text-purple-800 hover:bg-purple-200 transition-colors"
                                style={{ cursor: 'pointer' }}
                                onClick={() => {
                                  console.log('🎯 Skill clicked:', skill);
                                  handleSkillClick(skill);
                                }}
                              >
                                {skill}
                              </Badge>
                            ))}
                          </div>
                        </div>
                      )}
                      
                      {/* Show raw skills if no categorized skills */}
                      {skills.structural_design.length === 0 && skills.electrical_design.length === 0 && skills.coordination.length === 0 && employee.raw_technical_skills && (
                        <div>
                          <p className="text-sm text-orange-600 font-medium mb-2">Raw Skills Description:</p>
                          <p className="text-sm text-muted-foreground">{employee.raw_technical_skills}</p>
                        </div>
                      )}
                    </>
                  );
                })()}
              </div>
              
              {/* If no skills at all */}
              {(!employee.skills && !employee.technical_skills && !employee.raw_technical_skills) && (
                <p className="text-muted-foreground">No technical skills listed</p>
              )}
            </CardContent>
          </Card>
        </TabsContent>
        
        <TabsContent value="tasks" className="space-y-4">
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <CheckCircle2 className="h-5 w-5 text-primary" />
                Assigned Tasks ({assignedTasks.length})
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              {assignedTasks.length > 0 ? (
                assignedTasks.map((task, idx) => {
                  const duration = calculateDuration(task.time_assigned || task.assigned_at, task.completed_at);
                  const isActive = task.status === 'ASSIGNED';
                  
                  return (
                    <div key={idx} className="flex items-start justify-between p-4 rounded-lg bg-secondary/50 border border-border">
                      <div className="flex-1">
                        <p className="font-medium">{(task as any).title || task.task_assigned || task.task_description || 'Unnamed Task'}</p>
                        {task.original_filename && task.original_filename !== "General Task" && (
                          <div className="flex items-center gap-2 mt-2">
                            <Badge variant="outline" className="text-xs bg-blue-50 border-blue-200">
                              📄 {task.original_filename}
                            </Badge>
                            {task.client_name && (
                              <Badge variant="outline" className="text-xs">
                                👤 {task.client_name}
                              </Badge>
                            )}
                            {task.project_name && (
                              <Badge variant="secondary" className="text-xs">
                                📁 {task.project_name}
                              </Badge>
                            )}
                          </div>
                        )}
                        {task.permit_file_id && (
                          <span className="text-xs text-blue-600 mt-1 block">File ID: {task.permit_file_id}</span>
                        )}
                        <div className="flex items-center gap-4 mt-2 text-sm text-muted-foreground">
                          <span>Assigned {formatDate(task.time_assigned || task.assigned_at)}</span>
                          {task.completed_at && (
                            <span>Completed {formatDate(task.completed_at)}</span>
                          )}
                          {duration !== 'N/A' && (
                            <span className="flex items-center gap-1">
                              <Timer className="h-3 w-3" />
                              Duration: {duration}
                            </span>
                          )}
                        </div>
                        <p className="text-xs text-muted-foreground mt-1">
                          By: {task.assigned_by}
                        </p>
                      </div>
                      <div className="flex flex-col gap-2 items-end">
                        <Badge variant={task.status === 'ASSIGNED' ? 'warning' : 'success'}>
                          {task.status}
                        </Badge>
                        {isActive && (
                          <Badge variant="outline" className="text-xs">
                            Active
                          </Badge>
                        )}
                      </div>
                    </div>
                  );
                })
              ) : (
                <div className="text-center py-8 text-muted-foreground">
                  <p>No tasks assigned yet</p>
                </div>
              )}
            </CardContent>
          </Card>
        </TabsContent>
        
        <TabsContent value="performance" className="space-y-4">
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <BarChart3 className="h-5 w-5 text-primary" />
                Task Performance Metrics
              </CardTitle>
            </CardHeader>
            <CardContent>
              {taskStats ? (
                <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                  <div className="text-center p-4 rounded-lg bg-secondary/50">
                    <p className="text-2xl font-bold text-primary">{taskStats.total_assigned}</p>
                    <p className="text-sm text-muted-foreground mt-1">Total Assigned</p>
                  </div>
                  <div className="text-center p-4 rounded-lg bg-secondary/50">
                    <p className="text-2xl font-bold text-success">{taskStats.total_completed}</p>
                    <p className="text-sm text-muted-foreground mt-1">Completed</p>
                  </div>
                  <div className="text-center p-4 rounded-lg bg-secondary/50">
                    <p className="text-2xl font-bold text-warning">{taskStats.pending_tasks}</p>
                    <p className="text-sm text-muted-foreground mt-1">Pending</p>
                  </div>
                  <div className="text-center p-4 rounded-lg bg-secondary/50">
                    <p className="text-2xl font-bold text-info">{taskStats.completion_rate}%</p>
                    <p className="text-sm text-muted-foreground mt-1">Completion Rate</p>
                  </div>
                  <div className="text-center p-4 rounded-lg bg-secondary/50">
                    <p className="text-2xl font-bold text-primary">{taskStats.total_hours_worked}h</p>
                    <p className="text-sm text-muted-foreground mt-1">Total Hours</p>
                  </div>
                  <div className="text-center p-4 rounded-lg bg-secondary/50">
                    <p className="text-2xl font-bold text-primary">{taskStats.average_hours_per_task}h</p>
                    <p className="text-sm text-muted-foreground mt-1">Avg Hours/Task</p>
                  </div>
                </div>
              ) : (
                <div className="text-center py-8">
                  <p className="text-muted-foreground">Loading performance metrics...</p>
                </div>
              )}
            </CardContent>
          </Card>

          {/* Task Timing Analysis */}
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <Timer className="h-5 w-5 text-primary" />
                Completed Tasks Timing Analysis ({completedTasks.length})
              </CardTitle>
            </CardHeader>
            <CardContent>
              {completedTasks.length > 0 ? (
                <div className="space-y-4">
                  {completedTasks.map((task, idx) => {
                    const durationMinutes = calculateDurationMinutes(task.assigned_at, task.completed_at);
                    const duration = calculateDuration(task.assigned_at, task.completed_at);
                    
                    // Get performance metrics (using generic task standards since we don't have stage info)
                    const idealMinutes = 120; // Generic 2-hour standard
                    const maxMinutes = 240;   // Generic 4-hour max
                    const performance = getTimingPerformanceColor(durationMinutes, idealMinutes, maxMinutes);
                    
                    return (
                      <div key={idx} className="border rounded-lg p-4">
                        <div className="flex items-start justify-between mb-3">
                          <div className="flex-1">
                            <h4 className="font-medium text-base">{task.title}</h4>
                            <p className="text-sm text-muted-foreground mt-1">{task.description}</p>
                            {task.original_filename && task.original_filename !== "General Task" && (
                              <div className="flex items-center gap-2 mt-2">
                                <Badge variant="outline" className="text-xs bg-blue-50 border-blue-200">
                                  📄 {task.original_filename}
                                </Badge>
                                {task.client_name && (
                                  <Badge variant="outline" className="text-xs">
                                    👤 Client: {task.client_name}
                                  </Badge>
                                )}
                                {task.project_name && (
                                  <Badge variant="secondary" className="text-xs">
                                    📁 Project: {task.project_name}
                                  </Badge>
                                )}
                              </div>
                            )}
                            {task.permit_file_id && (
                              <span className="text-xs text-blue-600 mt-1 block">File ID: {task.permit_file_id}</span>
                            )}
                          </div>
                          <Badge variant={performance.variant} className="ml-4">
                            {performance.status}
                          </Badge>
                        </div>
                        
                        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 text-sm">
                          <div className="flex items-center gap-2">
                            <Calendar className="h-4 w-4 text-muted-foreground" />
                            <div>
                              <p className="text-muted-foreground">Assigned</p>
                              <p className="font-medium">{formatDate(task.assigned_at)}</p>
                            </div>
                          </div>
                          
                          <div className="flex items-center gap-2">
                            <CheckCircle2 className="h-4 w-4 text-muted-foreground" />
                            <div>
                              <p className="text-muted-foreground">Completed</p>
                              <p className="font-medium">{formatDate(task.completed_at)}</p>
                            </div>
                          </div>
                          
                          <div className="flex items-center gap-2">
                            <Timer className="h-4 w-4 text-muted-foreground" />
                            <div>
                              <p className="text-muted-foreground">Duration</p>
                              <p className={`font-medium ${performance.color}`}>
                                {duration}
                              </p>
                            </div>
                          </div>
                        </div>
                        
                        {/* Performance Comparison */}
                        <div className="mt-4 pt-4 border-t">
                          <div className="flex items-center justify-between mb-2">
                            <span className="text-sm text-muted-foreground">Performance Analysis</span>
                            <span className={`text-sm font-medium ${performance.color}`}>
                              {performance.status}
                            </span>
                          </div>
                          <div className="grid grid-cols-3 gap-4 text-xs">
                            <div className="text-center">
                              <p className="text-green-600 font-medium">Ideal</p>
                              <p>{idealMinutes}m</p>
                            </div>
                            <div className="text-center">
                              <p className="text-yellow-600 font-medium">Actual</p>
                              <p className={performance.color}>{durationMinutes}m</p>
                            </div>
                            <div className="text-center">
                              <p className="text-red-600 font-medium">Max</p>
                              <p>{maxMinutes}m</p>
                            </div>
                          </div>
                          
                          {/* Progress bar showing performance */}
                          <div className="mt-3">
                            <div className="flex justify-between text-xs text-muted-foreground mb-1">
                              <span>Performance</span>
                              <span>{Math.min(100, Math.round((durationMinutes / maxMinutes) * 100))}% of max time</span>
                            </div>
                            <Progress 
                              value={Math.min(100, (durationMinutes / maxMinutes) * 100)} 
                              className={`h-2 ${durationMinutes > maxMinutes ? 'bg-red-100' : durationMinutes > idealMinutes ? 'bg-yellow-100' : 'bg-green-100'}`}
                            />
                          </div>
                        </div>
                        
                        {/* Skills */}
                        {task.skills_required && task.skills_required.length > 0 && (
                          <div className="mt-4 pt-4 border-t">
                            <p className="text-sm text-muted-foreground mb-2">Skills Used:</p>
                            <div className="flex flex-wrap gap-1">
                              {task.skills_required.map((skill: string, skillIdx: number) => (
                                <Badge key={skillIdx} variant="outline" className="text-xs">
                                  {skill}
                                </Badge>
                              ))}
                            </div>
                          </div>
                        )}
                      </div>
                    );
                  })}
                </div>
              ) : (
                <div className="text-center py-8 text-muted-foreground">
                  <Timer className="h-12 w-12 mx-auto mb-4 opacity-50" />
                  <p>No completed tasks to analyze</p>
                  <p className="text-sm mt-2">Completed tasks will appear here with timing analysis</p>
                </div>
              )}
            </CardContent>
          </Card>
          
          {taskStats && taskStats.recent_tasks && taskStats.recent_tasks.length > 0 && (
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <TrendingUp className="h-5 w-5 text-primary" />
                  Recent Task Activity
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-3">
                  {taskStats.recent_tasks.map((task: any, index: number) => {
                    const duration = calculateDuration(task.assigned_at, task.completed_at);
                    
                    return (
                      <div key={index} className="flex items-center justify-between p-3 rounded-lg border">
                        <div>
                          <p className="font-medium">{task.title}</p>
                          <p className="text-sm text-muted-foreground">
                            {task.assigned_at ? formatDate(task.assigned_at) : 'No date'}
                            {task.completed_at ? ` • Completed ${formatDate(task.completed_at)}` : ''}
                            {duration !== 'N/A' && ` • Duration: ${duration}`}
                          </p>
                        </div>
                        <Badge variant={task.status === 'COMPLETED' ? 'success' : 'warning'}>
                          {task.status}
                        </Badge>
                      </div>
                    );
                  })}
                </div>
              </CardContent>
            </Card>
          )}
        </TabsContent>
      </Tabs>
      
      {/* Skill Modal */}
      <Dialog open={showSkillModal} onOpenChange={setShowSkillModal}>
        <DialogContent className="max-w-4xl max-h-[80vh] overflow-y-auto">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              <Star className="h-5 w-5 text-primary" />
              Employees with Skill: {selectedSkill}
            </DialogTitle>
          </DialogHeader>
          <div className="space-y-4">
            {employeesWithSkill.length > 0 ? (
              <div className="grid gap-4">
                <p className="text-sm text-muted-foreground">
                  Found {employeesWithSkill.length} employee(s) with this skill
                </p>
                {employeesWithSkill.map((emp) => (
                  <Card key={emp.employee_code} className="p-4">
                    <div className="flex items-center justify-between">
                      <div className="flex items-center gap-4">
                        <div className="w-12 h-12 bg-blue-100 rounded-full flex items-center justify-center">
                          <User className="w-6 h-6 text-blue-600" />
                        </div>
                        <div>
                          <h4 className="font-semibold">{emp.employee_name}</h4>
                          <p className="text-sm text-muted-foreground">{emp.employee_code}</p>
                          <p className="text-sm">{emp.current_role}</p>
                        </div>
                      </div>
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={() => navigate(`/employee/${emp.employee_code}`)}
                      >
                        View Profile
                      </Button>
                    </div>
                  </Card>
                ))}
              </div>
            ) : (
              <div className="text-center py-8">
                <Star className="w-16 h-16 mx-auto mb-4 opacity-50" />
                <p className="text-lg font-medium">No employees found with this skill</p>
                <p className="text-sm text-muted-foreground">Try searching for a different skill</p>
              </div>
            )}
          </div>
        </DialogContent>
      </Dialog>
    </div>
  );
}

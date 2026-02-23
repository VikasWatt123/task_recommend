import React, { useState, useEffect, useMemo } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Progress } from '@/components/ui/progress';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { 
  User, 
  Clock, 
  TrendingUp, 
  AlertTriangle, 
  CheckCircle, 
  Timer,
  Award,
  Target,
  Activity,
  Download,
  Users,
  Filter,
  Search,
  Calendar,
  BarChart3
} from 'lucide-react';
import { api } from '@/lib/api';
import { getFileId, getFileDisplayName, isStandaloneTask } from '@/utils/taskHelpers';
import { 
  Employee, 
  PerformanceData, 
  SLAReport,
  PerformanceResponse,
  SLAReportResponse,
  Task,
  TaskBoardData,
  EmployeeTasks
} from '@/types/stageTracking';

type EmployeeTaskApiItem = {
  task_id?: string;
  title?: string;
  description?: string;
  task_assigned?: string;
  status?: string;
  assigned_at?: string;
  time_assigned?: string;
  assigned_by?: string;
  completion_time?: string;
  completed_at?: string;
  date_assigned?: string;
  hours_taken?: number;
  file_id?: string;
  permit_file_id?: string; // Backward compatibility
  tracking_mode?: 'FILE_BASED' | 'STANDALONE';
  client_name?: string;
  project_name?: string;
  original_filename?: string;
  employee_code?: string;
};

type EmployeeTasksApiResponse = {
  assigned_tasks?: EmployeeTaskApiItem[];
  completed_tasks?: EmployeeTaskApiItem[];
};

const EnhancedEmployeePerformanceDashboard: React.FC = () => {
  const [employees, setEmployees] = useState<Employee[]>([]);
  const [reportingManagers, setReportingManagers] = useState<Employee[]>([]);
  const [selectedEmployee, setSelectedEmployee] = useState('');
  const [selectedReportingManager, setSelectedReportingManager] = useState('');
  const [performanceData, setPerformanceData] = useState<PerformanceData | null>(null);
  const [slaReport, setSlaReport] = useState<SLAReport | null>(null);
  const [employeeTasks, setEmployeeTasks] = useState<EmployeeTaskApiItem[]>([]);
  const [completedTasks, setCompletedTasks] = useState<EmployeeTaskApiItem[]>([]);
  const [taskBoardData, setTaskBoardData] = useState<TaskBoardData | null>(null);
  const [loading, setLoading] = useState(true);
  const [timeRange, setTimeRange] = useState('30'); // days
  const [searchTerm, setSearchTerm] = useState('');

  // Helper functions for timing analysis
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

  const formatDate = (dateString: string): string => {
    if (!dateString) return 'No date';
    
    try {
      const date = new Date(dateString);
      if (isNaN(date.getTime())) return 'Invalid date';
      
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

  // Stage time standards (from backend)
  const STAGE_TIME_STANDARDS = {
    'PRELIMS': { ideal: 20, max: 30, display: 'Prelims' },
    'PRODUCTION': { ideal: 210, max: 240, display: 'Production' },
    'COMPLETED': { ideal: 0, max: 5, display: 'Completed' },
    'QC': { ideal: 90, max: 120, display: 'Quality Control' },
    'DELIVERED': { ideal: 0, max: 5, display: 'Delivered' }
  };

  const getTimingPerformanceColor = (actualMinutes: number, idealMinutes: number, maxMinutes: number): { color: string, status: string, variant: 'default' | 'secondary' | 'destructive' | 'outline' } => {
    if (actualMinutes <= idealMinutes) {
      return { color: 'text-green-600', status: 'Excellent', variant: 'default' as const };
    } else if (actualMinutes <= maxMinutes) {
      return { color: 'text-yellow-600', status: 'Good', variant: 'secondary' as const };
    } else {
      return { color: 'text-red-600', status: 'Overdue', variant: 'destructive' as const };
    }
  };

  useEffect(() => {
    fetchEmployees();
    fetchSLAReport();
  }, []);

  useEffect(() => {
    if (selectedEmployee) {
      fetchPerformanceData(selectedEmployee);
      fetchEmployeeTasks(selectedEmployee);
      fetchCompletedTasks(selectedEmployee);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps -- preserve current fetch timing keyed to selectedEmployee/timeRange
  }, [selectedEmployee, timeRange]);

  useEffect(() => {
    // Clear selected employee when manager changes
    setSelectedEmployee('');
    setPerformanceData(null);
    setEmployeeTasks([]);
    setCompletedTasks([]);
    
    if (selectedReportingManager && selectedReportingManager !== 'all') {
      // Auto-select first direct report if none selected
      const directReports = getDirectReports(selectedReportingManager);
      if (directReports.length > 0) {
        setSelectedEmployee(directReports[0].employee_code);
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps -- preserve current behavior: only re-evaluate on manager change
  }, [selectedReportingManager]);

  const fetchEmployees = async () => {
    try {
      const response = await api.get<Employee[]>('/employees/');
      const employeesData = response.data || [];
      setEmployees(employeesData);
      
      // Extract unique reporting managers - people who are actually managers of others
      const managerMap = new Map<string, Employee>();
      
      employeesData.forEach(emp => {
        // Add reporting_manager if it exists
        if (emp.reporting_manager) {
          const manager = employeesData.find(e => e.employee_code === emp.reporting_manager);
          if (manager && !managerMap.has(emp.reporting_manager)) {
            managerMap.set(emp.reporting_manager, manager);
          }
        }
        // Add reporting_manager_2 if it exists
        if (emp.reporting_manager_2) {
          const manager = employeesData.find(e => e.employee_code === emp.reporting_manager_2);
          if (manager && !managerMap.has(emp.reporting_manager_2)) {
            managerMap.set(emp.reporting_manager_2, manager);
          }
        }
      });
      
      const managersList = Array.from(managerMap.values());
      console.log(`Found ${managersList.length} actual reporting managers`);
      setReportingManagers(managersList);
    } catch (error) {
      console.error('Failed to fetch employees:', error);
    }
  };

  const fetchPerformanceData = async (employeeCode: string) => {
    try {
      setLoading(true);
      const response = await api.get<PerformanceResponse>(`/stage-tracking/employee/${employeeCode}/performance?days=${timeRange}`);
      setPerformanceData(response.data.performance);
    } catch (error) {
      console.error('Failed to fetch performance data:', error);
    } finally {
      setLoading(false);
    }
  };

  const fetchEmployeeTasks = async (employeeCode: string) => {
    try {
      // Use the same endpoint as employee page for consistency
      const response = await api.get<EmployeeTasksApiResponse>(`/employee-tasks/${employeeCode}`);
      const tasksData = response.data;
      
      // Combine assigned and completed tasks for full picture
      const allTasks = [
        ...(tasksData.assigned_tasks || []),
        ...(tasksData.completed_tasks || [])
      ];
      
      setEmployeeTasks(allTasks);
      console.log(`[Dashboard] Loaded ${allTasks.length} total tasks for ${employeeCode}`);
    } catch (error) {
      console.error('Failed to fetch employee tasks:', error);
    }
  };

  const fetchCompletedTasks = async (employeeCode: string) => {
    try {
      // Use the same endpoint as employee page for consistency
      const response = await api.get<EmployeeTasksApiResponse>(`/employee-tasks/${employeeCode}`);
      const tasksData = response.data;
      
      // Use completed tasks from the same source
      setCompletedTasks(tasksData.completed_tasks || []);
      console.log(`[Dashboard] Loaded ${tasksData.completed_tasks?.length || 0} completed tasks for ${employeeCode}`);
    } catch (error) {
      console.error('Failed to fetch completed tasks:', error);
    }
  };

  const fetchTaskBoardData = async () => {
    try {
      const response = await api.get<TaskBoardData>('/tasks/board');
      setTaskBoardData(response.data);
    } catch (error) {
      console.error('Failed to fetch task board data:', error);
    }
  };

  const fetchSLAReport = async () => {
    try {
      const response = await api.get<SLAReportResponse>('/stage-tracking/sla-report?days=30');
      setSlaReport(response.data.report);
    } catch (error) {
      console.error('Failed to fetch SLA report:', error);
    }
  };

  const formatDuration = (minutes: number) => {
    if (minutes < 60) return `${minutes}m`;
    const hours = Math.floor(minutes / 60);
    const mins = minutes % 60;
    return `${hours}h ${mins}m`;
  };

  const getPerformanceColor = (score: number, type: 'penalty' | 'duration') => {
    if (type === 'penalty') {
      if (score === 0) return 'text-green-600';
      if (score < 5) return 'text-yellow-600';
      if (score < 10) return 'text-orange-600';
      return 'text-red-600';
    } else {
      if (score < 30) return 'text-green-600';
      if (score < 60) return 'text-yellow-600';
      if (score < 120) return 'text-orange-600';
      return 'text-red-600';
    }
  };

  const getPerformanceBadge = (score: number, type: 'penalty' | 'duration') => {
    if (type === 'penalty') {
      if (score === 0) return { text: 'Excellent', color: 'bg-green-100 text-green-800' };
      if (score < 5) return { text: 'Good', color: 'bg-yellow-100 text-yellow-800' };
      if (score < 10) return { text: 'Needs Improvement', color: 'bg-orange-100 text-orange-800' };
      return { text: 'Poor', color: 'bg-red-100 text-red-800' };
    } else {
      if (score < 30) return { text: 'Fast', color: 'bg-green-100 text-green-800' };
      if (score < 60) return { text: 'Normal', color: 'bg-yellow-100 text-yellow-800' };
      if (score < 120) return { text: 'Slow', color: 'bg-orange-100 text-orange-800' };
      return { text: 'Very Slow', color: 'bg-red-100 text-red-800' };
    }
  };

  const filteredEmployees = employees.filter(emp => {
    const matchesSearch = emp.employee_name.toLowerCase().includes(searchTerm.toLowerCase()) ||
                         emp.employee_code.toLowerCase().includes(searchTerm.toLowerCase());
    const matchesManager = !selectedReportingManager || 
                          selectedReportingManager === 'all' ||
                          emp.reporting_manager === selectedReportingManager || 
                          emp.reporting_manager_2 === selectedReportingManager;
    return matchesSearch && matchesManager;
  });

  // Get employees who report to the selected manager
  const getDirectReports = (managerCode: string) => {
    return employees.filter(emp => 
      emp.reporting_manager === managerCode || 
      emp.reporting_manager_2 === managerCode
    );
  };

  // Get employees to show in dropdown based on selected manager
  const getEmployeesForSelection = () => {
    if (!selectedReportingManager || selectedReportingManager === 'all') {
      return filteredEmployees;
    }
    return getDirectReports(selectedReportingManager).filter(emp =>
      emp.employee_name.toLowerCase().includes(searchTerm.toLowerCase()) ||
      emp.employee_code.toLowerCase().includes(searchTerm.toLowerCase())
    );
  };

  const exportPerformanceData = () => {
    if (!performanceData) return;
    
    const csvContent = [
      ['Employee Code', 'Employee Name', 'Active Assignments', 'Completed Stages', 'Total Penalties', 'Avg Duration (min)'],
      [
        performanceData.employee_code,
        employees.find(e => e.employee_code === performanceData.employee_code)?.employee_name || '',
        performanceData.active_assignments.toString(),
        performanceData.completed_stages.toString(),
        performanceData.total_penalty_points.toString(),
        performanceData.average_stage_duration_minutes.toString()
      ]
    ].map(row => row.join(',')).join('\n');

    const blob = new Blob([csvContent], { type: 'text/csv' });
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `performance_${performanceData.employee_code}_${new Date().toISOString().split('T')[0]}.csv`;
    a.click();
    window.URL.revokeObjectURL(url);
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-2xl font-bold">Enhanced Employee Performance Dashboard</h1>
          <p className="text-gray-600">Track performance, tasks, and SLA compliance with advanced filtering</p>
        </div>
        {performanceData && (
          <Button onClick={exportPerformanceData} variant="outline" size="sm">
            <Download className="w-4 h-4 mr-2" />
            Export Data
          </Button>
        )}
      </div>

      {/* Filters Section */}
      <Card>
        <CardContent className="p-4">
          <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
            <div>
              <Label>Reporting Manager</Label>
              <Select value={selectedReportingManager} onValueChange={setSelectedReportingManager}>
                <SelectTrigger>
                  <SelectValue placeholder="Filter by manager" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All Managers</SelectItem>
                  {reportingManagers.map(manager => (
                    <SelectItem key={manager.employee_code} value={manager.employee_code}>
                      {manager.employee_name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              {selectedReportingManager && selectedReportingManager !== 'all' && (
                <p className="text-xs text-muted-foreground mt-1">
                  Showing {getDirectReports(selectedReportingManager).length} direct reports
                </p>
              )}
            </div>
            <div>
              <Label htmlFor="employee">Select Employee</Label>
              <Select value={selectedEmployee} onValueChange={setSelectedEmployee}>
                <SelectTrigger>
                  <SelectValue placeholder="Choose an employee" />
                </SelectTrigger>
                <SelectContent>
                  {getEmployeesForSelection().map(emp => (
                    <SelectItem key={emp.employee_code} value={emp.employee_code}>
                      {emp.employee_name} - {emp.current_role}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div>
              <Label htmlFor="time-range">Time Range (days)</Label>
              <Select value={timeRange} onValueChange={setTimeRange}>
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="7">Last 7 days</SelectItem>
                  <SelectItem value="30">Last 30 days</SelectItem>
                  <SelectItem value="90">Last 90 days</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div className="flex items-end">
              <div className="relative w-full">
                <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 w-4 h-4" />
                <Input
                  placeholder="Search employees..."
                  value={searchTerm}
                  onChange={(e) => setSearchTerm(e.target.value)}
                  className="pl-10"
                />
              </div>
            </div>
          </div>
        </CardContent>
      </Card>

      {selectedEmployee && performanceData && (
        <>
          {/* Performance Summary Cards */}
          <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
            <Card>
              <CardContent className="p-4">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-sm text-gray-600">Active Assignments</p>
                    <p className="text-2xl font-bold">{performanceData.active_assignments}</p>
                  </div>
                  <User className="w-8 h-8 text-blue-600 opacity-50" />
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardContent className="p-4">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-sm text-gray-600">Completed Stages</p>
                    <p className="text-2xl font-bold text-green-600">{performanceData.completed_stages}</p>
                  </div>
                  <CheckCircle className="w-8 h-8 text-green-600 opacity-50" />
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardContent className="p-4">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-sm text-gray-600">Penalty Points</p>
                    <p className={`text-2xl font-bold ${getPerformanceColor(performanceData.total_penalty_points, 'penalty')}`}>
                      {performanceData.total_penalty_points}
                    </p>
                  </div>
                  <AlertTriangle className="w-8 h-8 text-red-600 opacity-50" />
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardContent className="p-4">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-sm text-gray-600">Avg Duration</p>
                    <p className={`text-2xl font-bold ${getPerformanceColor(performanceData.average_stage_duration_minutes, 'duration')}`}>
                      {formatDuration(performanceData.average_stage_duration_minutes)}
                    </p>
                  </div>
                  <Clock className="w-8 h-8 text-orange-600 opacity-50" />
                </div>
              </CardContent>
            </Card>
          </div>

          {/* Task Board Integration */}
          <Tabs defaultValue="performance" className="w-full">
            <TabsList className="grid w-full grid-cols-4">
              <TabsTrigger value="performance">Performance Metrics</TabsTrigger>
              <TabsTrigger value="tasks">Current Tasks ({employeeTasks.length})</TabsTrigger>
              <TabsTrigger value="timing">Task Timing Analysis ({completedTasks.length})</TabsTrigger>
              <TabsTrigger value="activity">Complete Activity</TabsTrigger>
            </TabsList>

            <TabsContent value="performance" className="mt-4">
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <Card>
                  <CardHeader>
                    <CardTitle className="text-lg">Performance Rating</CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="space-y-3">
                      <div className="flex items-center justify-between">
                        <span>Penalty Performance</span>
                        <Badge className={getPerformanceBadge(performanceData.total_penalty_points, 'penalty').color}>
                          {getPerformanceBadge(performanceData.total_penalty_points, 'penalty').text}
                        </Badge>
                      </div>
                      <div className="flex items-center justify-between">
                        <span>Speed Performance</span>
                        <Badge className={getPerformanceBadge(performanceData.average_stage_duration_minutes, 'duration').color}>
                          {getPerformanceBadge(performanceData.average_stage_duration_minutes, 'duration').text}
                        </Badge>
                      </div>
                    </div>
                  </CardContent>
                </Card>

                <Card>
                  <CardHeader>
                    <CardTitle className="text-lg">Efficiency Metrics</CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="space-y-3">
                      <div>
                        <div className="flex justify-between text-sm mb-1">
                          <span>Completion Rate</span>
                          <span>
                            {performanceData.completed_stages > 0 
                              ? Math.round((performanceData.completed_stages / (performanceData.completed_stages + performanceData.active_assignments)) * 100)
                              : 0}%
                          </span>
                        </div>
                        <Progress value={performanceData.completed_stages > 0 
                          ? (performanceData.completed_stages / (performanceData.completed_stages + performanceData.active_assignments)) * 100 
                          : 0} 
                        className="h-2" />
                      </div>
                      <div>
                        <div className="flex justify-between text-sm mb-1">
                          <span>Penalty Ratio</span>
                          <span>
                            {performanceData.completed_stages > 0 
                              ? Math.round((performanceData.total_penalty_points / performanceData.completed_stages) * 10) / 10
                              : 0} pts/stage
                          </span>
                        </div>
                        <Progress 
                          value={Math.min((performanceData.total_penalty_points / Math.max(performanceData.completed_stages, 1)) * 10, 100)} 
                          className="h-2"
                        />
                      </div>
                    </div>
                  </CardContent>
                </Card>
              </div>
            </TabsContent>

            <TabsContent value="tasks" className="mt-4">
              <Card>
                <CardHeader>
                  <CardTitle>Current Task Board</CardTitle>
                </CardHeader>
                <CardContent>
                  {employeeTasks.length === 0 ? (
                    <div className="text-center text-gray-500 py-8">
                      <Activity className="w-12 h-12 mx-auto mb-4 opacity-50" />
                      <p>No tasks assigned</p>
                    </div>
                  ) : (
                    <div className="space-y-3">
                      {employeeTasks.map((task, index) => {
                        const isActive = task.status === 'ASSIGNED';
                        const isCompleted = task.status === 'COMPLETED';
                        
                        // Use enhanced title from backend with fallbacks
                        const getTaskTitle = (t: EmployeeTaskApiItem) => {
                          // Prefer the enhanced title from backend (should now include manual titles like "structural Loading")
                          if (t.title && 
                              t.title !== 'Untitled Task' && 
                              t.title !== 'Current Assigned Task' && 
                              t.title !== 'Unknown Task') {
                            return t.title;
                          }
                          
                          // Fallback to original filename with context
                          if (t.original_filename && t.original_filename !== 'General Task') {
                            const taskDesc = (t.description || '').toLowerCase();
                            if (taskDesc.includes('review') || taskDesc.includes('prelims')) {
                              return `Review: ${t.original_filename}`;
                            } else if (taskDesc.includes('production') || taskDesc.includes('produce')) {
                              return `Production: ${t.original_filename}`;
                            } else if (taskDesc.includes('qc') || taskDesc.includes('quality')) {
                              return `QC: ${t.original_filename}`;
                            } else {
                              return `Task: ${t.original_filename}`;
                            }
                          }
                          
                          // Final fallback to description or task_assigned
                          return t.description || t.task_assigned || 'Untitled Task';
                        };

                        return (
                          <div key={index} className="border rounded-lg p-3">
                            <div className="flex items-start justify-between mb-2">
                              <div className="flex-1">
                                <h4 className="font-medium">{getTaskTitle(task)}</h4>
                                <p className="text-sm text-gray-600">{task.description || task.task_assigned}</p>
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
                                  <p className="text-xs text-blue-600 mt-1">File ID: {task.permit_file_id}</p>
                                )}
                              </div>
                              <div className="flex items-center gap-2">
                                <Badge variant={isCompleted ? 'default' : isActive ? 'secondary' : 'outline'}>
                                  {task.status}
                                </Badge>
                                {isActive && (
                                  <Badge variant="outline" className="text-xs">
                                    Active
                                  </Badge>
                                )}
                              </div>
                            </div>
                            <div className="text-sm text-gray-600 space-y-1">
                              <div className="flex justify-between">
                                <span>Assigned: {formatDate(task.time_assigned || task.assigned_at)}</span>
                                {task.assigned_by && (
                                  <span>By: {task.assigned_by}</span>
                                )}
                              </div>
                              {task.completion_time && (
                                <div>Completed: {formatDate(task.completion_time)}</div>
                              )}
                              {task.hours_taken && (
                                <div>Duration: {task.hours_taken}h</div>
                              )}
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  )}
                </CardContent>
              </Card>
            </TabsContent>

            <TabsContent value="timing" className="mt-4">
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <Timer className="h-5 w-5 text-primary" />
                    Completed Tasks Timing Analysis
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  {completedTasks.length > 0 ? (
                    <div className="space-y-4">
                      {/* Summary Statistics */}
                      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6">
                        <div className="text-center p-4 rounded-lg bg-blue-50 border border-blue-200">
                          <p className="text-2xl font-bold text-blue-600">{completedTasks.length}</p>
                          <p className="text-sm text-blue-600 mt-1">Total Completed</p>
                        </div>
                        <div className="text-center p-4 rounded-lg bg-green-50 border border-green-200">
                          <p className="text-2xl font-bold text-green-600">
                            {completedTasks.length > 0 ? Math.round(
                              completedTasks.reduce((acc: number, task) => 
                                acc + (task.hours_taken || 0), 0
                            ) / completedTasks.length
                            ) : 0}h
                          </p>
                          <p className="text-sm text-green-600 mt-1">Average Duration</p>
                        </div>
                        <div className="text-center p-4 rounded-lg bg-purple-50 border border-purple-200">
                          <p className="text-2xl font-bold text-purple-600">
                            {completedTasks.filter((task) => {
                              const hours = task.hours_taken || 0;
                              return hours <= 2; // Within 2 hours
                            }).length}
                          </p>
                          <p className="text-sm text-purple-600 mt-1">On-Time Tasks</p>
                        </div>
                      </div>

                      {/* Individual Task Analysis */}
                      {completedTasks.map((task, index) => {
                        const hoursTaken = task.hours_taken || 0;
                        const durationMinutes = hoursTaken * 60;
                        const duration = hoursTaken > 0 ? `${hoursTaken}h` : 'N/A';
                        
                        // Use task-specific standards
                        const idealHours = 2; // 2 hours
                        const maxHours = 4;   // 4 hours
                        const performance = getTimingPerformanceColor(durationMinutes, idealHours * 60, maxHours * 60);
                        
                        // Use enhanced title from backend with fallbacks
                        const getTaskTitle = (t: EmployeeTaskApiItem) => {
                          // Prefer the enhanced title from backend (should now include manual titles like "structural Loading")
                          if (t.title && 
                              t.title !== 'Untitled Task' && 
                              t.title !== 'Current Assigned Task' && 
                              t.title !== 'Unknown Task') {
                            return t.title;
                          }
                          
                          // Fallback to original filename with context
                          if (t.original_filename && t.original_filename !== 'General Task') {
                            const taskDesc = (t.description || '').toLowerCase();
                            if (taskDesc.includes('review') || taskDesc.includes('prelims')) {
                              return `Review: ${t.original_filename}`;
                            } else if (taskDesc.includes('production') || taskDesc.includes('produce')) {
                              return `Production: ${t.original_filename}`;
                            } else if (taskDesc.includes('qc') || taskDesc.includes('quality')) {
                              return `QC: ${t.original_filename}`;
                            } else {
                              return `Task: ${t.original_filename}`;
                            }
                          }
                          
                          // Final fallback to description or task_assigned
                          return t.description || t.task_assigned || 'Untitled Task';
                        };

                        return (
                          <div key={index} className="border rounded-lg p-4">
                            <div className="flex items-start justify-between mb-3">
                              <div className="flex-1">
                                <h4 className="font-medium text-base">{getTaskTitle(task)}</h4>
                                <p className="text-sm text-gray-600 mt-1">{task.description || task.task_assigned}</p>
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
                                {!isStandaloneTask(task) && (
                                  <p className="text-xs text-blue-600 mt-1">
                                    {getFileDisplayName(task)}
                                  </p>
                                )}
                                {task.tracking_mode && (
                                  <Badge variant="outline" className="text-xs mt-1">
                                    {task.tracking_mode === 'FILE_BASED' ? 'File-Based' : 'Standalone'}
                                  </Badge>
                                )}
                              </div>
                              <Badge variant={performance.variant} className="ml-4">
                                {performance.status}
                              </Badge>
                            </div>
                            
                            <div className="grid grid-cols-1 md:grid-cols-4 gap-4 text-sm mb-4">
                              <div className="flex items-center gap-2">
                                <Calendar className="h-4 w-4 text-gray-500" />
                                <div>
                                  <p className="text-gray-500">Assigned</p>
                                  <p className="font-medium">{formatDate(task.time_assigned || task.assigned_at)}</p>
                                </div>
                              </div>
                              
                              <div className="flex items-center gap-2">
                                <CheckCircle className="h-4 w-4 text-gray-500" />
                                <div>
                                  <p className="text-gray-500">Completed</p>
                                  <p className="font-medium">{formatDate(task.completion_time || task.completed_at)}</p>
                                </div>
                              </div>
                              
                              <div className="flex items-center gap-2">
                                <Timer className="h-4 w-4 text-gray-500" />
                                <div>
                                  <p className="text-gray-500">Duration</p>
                                  <p className={`font-medium ${performance.color}`}>
                                    {duration}
                                  </p>
                                </div>
                              </div>
                              
                              <div className="flex items-center gap-2">
                                <BarChart3 className="h-4 w-4 text-gray-500" />
                                <div>
                                  <p className="text-gray-500">Performance</p>
                                  <p className={`font-medium ${performance.color}`}>
                                    {performance.status}
                                  </p>
                                </div>
                              </div>
                            </div>
                            
                            {/* Performance Comparison */}
                            <div className="mt-4 pt-4 border-t">
                              <div className="flex items-center justify-between mb-2">
                                <span className="text-sm text-gray-500">Time Analysis</span>
                                <span className={`text-sm font-medium ${performance.color}`}>
                                  {hoursTaken <= idealHours ? 'Within Standard' : 
                                   hoursTaken <= maxHours ? 'Acceptable' : 'Exceeded Standard'}
                                </span>
                              </div>
                              <div className="grid grid-cols-3 gap-4 text-xs mb-3">
                                <div className="text-center">
                                  <p className="text-green-600 font-medium">Ideal</p>
                                  <p>{idealHours}h</p>
                                </div>
                                <div className="text-center">
                                  <p className="text-yellow-600 font-medium">Actual</p>
                                  <p className={performance.color}>{hoursTaken}h</p>
                                </div>
                                <div className="text-center">
                                  <p className="text-red-600 font-medium">Max</p>
                                  <p>{maxHours}h</p>
                                </div>
                              </div>
                              
                              {/* Progress bar */}
                              <div className="mt-3">
                                <div className="flex justify-between text-xs text-gray-500 mb-1">
                                  <span>Time Utilization</span>
                                  <span>{Math.min(100, Math.round((hoursTaken / maxHours) * 100))}% of max time</span>
                                </div>
                                <Progress 
                                  value={Math.min(100, (hoursTaken / maxHours) * 100)} 
                                  className={`h-2 ${
                                    hoursTaken > maxHours ? 'bg-red-100' : 
                                    hoursTaken > idealHours ? 'bg-yellow-100' : 'bg-green-100'
                                  }`}
                                />
                              </div>
                            </div>
                            
                            {/* Task Details */}
                            <div className="mt-4 pt-4 border-t">
                              <div className="grid grid-cols-2 gap-4 text-sm">
                                <div>
                                  <p className="text-gray-500">Assigned By</p>
                                  <p className="font-medium">{task.assigned_by || 'N/A'}</p>
                                </div>
                                <div>
                                  <p className="text-gray-500">Employee Code</p>
                                  <p className="font-medium">{task.employee_code || 'N/A'}</p>
                                </div>
                              </div>
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  ) : (
                    <div className="text-center py-12 text-gray-500">
                      <Timer className="w-16 h-16 mx-auto mb-4 opacity-50" />
                      <p className="text-lg font-medium">No completed tasks to analyze</p>
                      <p className="text-sm mt-2">Completed tasks will appear here with detailed timing analysis</p>
                    </div>
                  )}
                </CardContent>
              </Card>
            </TabsContent>

            <TabsContent value="activity" className="mt-4">
              <div className="space-y-4">
                <Card>
                  <CardHeader>
                    <CardTitle>Active Work</CardTitle>
                  </CardHeader>
                  <CardContent>
                    {performanceData.active_work.length === 0 ? (
                      <div className="text-center text-gray-500 py-8">
                        <Timer className="w-12 h-12 mx-auto mb-4 opacity-50" />
                        <p>No active work in this period</p>
                      </div>
                    ) : (
                      <div className="space-y-3">
                        {performanceData.active_work.map((work, index) => (
                          <div key={index} className="border rounded-lg p-3">
                            <div className="flex justify-between items-start mb-2">
                              <div>
                                <h4 className="font-medium">{work.file_id}</h4>
                                <p className="text-sm text-gray-600">{work.stage}</p>
                              </div>
                              <Badge variant="outline">Active</Badge>
                            </div>
                            <div className="text-sm text-gray-600 space-y-1">
                              <div>Assigned: {new Date(work.assigned_at).toLocaleString()}</div>
                              {work.started_at && (
                                <div>Started: {new Date(work.started_at).toLocaleString()}</div>
                              )}
                              {work.duration_minutes && (
                                <div>Duration: {formatDuration(work.duration_minutes)}</div>
                              )}
                            </div>
                          </div>
                        ))}
                      </div>
                    )}
                  </CardContent>
                </Card>

                <Card>
                  <CardHeader>
                    <CardTitle>Completed Work</CardTitle>
                  </CardHeader>
                  <CardContent>
                    {performanceData.completed_work.length === 0 ? (
                      <div className="text-center text-gray-500 py-8">
                        <CheckCircle className="w-12 h-12 mx-auto mb-4 opacity-50" />
                        <p>No completed work in this period</p>
                      </div>
                    ) : (
                      <div className="space-y-3">
                        {performanceData.completed_work.map((work, index) => (
                          <div key={index} className="border rounded-lg p-3">
                            <div className="flex justify-between items-start mb-2">
                              <div>
                                <h4 className="font-medium">{work.file_id}</h4>
                                <p className="text-sm text-gray-600">{work.stage}</p>
                              </div>
                              <div className="flex items-center gap-2">
                                <Badge variant="default">Completed</Badge>
                                {work.penalty_points > 0 && (
                                  <Badge variant="destructive">-{work.penalty_points} pts</Badge>
                                )}
                              </div>
                            </div>
                            <div className="text-sm text-gray-600 space-y-1">
                              <div>Completed: {new Date(work.completed_at).toLocaleString()}</div>
                              <div>Duration: {formatDuration(work.duration_minutes)}</div>
                            </div>
                          </div>
                        ))}
                      </div>
                    )}
                  </CardContent>
                </Card>
              </div>
            </TabsContent>
          </Tabs>
        </>
      )}

      {/* SLA Overview */}
      {slaReport && (
        <Card>
          <CardHeader>
            <CardTitle>Team SLA Overview (Last 30 Days)</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-2 md:grid-cols-5 gap-4 mb-4">
              <div className="text-center">
                <p className="text-2xl font-bold text-blue-600">{slaReport.total_stages}</p>
                <p className="text-sm text-gray-600">Total Stages</p>
              </div>
              <div className="text-center">
                <p className="text-2xl font-bold text-green-600">{slaReport.within_ideal}</p>
                <p className="text-sm text-gray-600">Within Ideal</p>
              </div>
              <div className="text-center">
                <p className="text-2xl font-bold text-yellow-600">{slaReport.over_ideal}</p>
                <p className="text-sm text-gray-600">Over Ideal</p>
              </div>
              <div className="text-center">
                <p className="text-2xl font-bold text-red-600">{slaReport.over_max}</p>
                <p className="text-sm text-gray-600">Over Max</p>
              </div>
              <div className="text-center">
                <p className="text-2xl font-bold text-red-800">{slaReport.escalations}</p>
                <p className="text-sm text-gray-600">Escalations</p>
              </div>
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  );
};

export default EnhancedEmployeePerformanceDashboard;

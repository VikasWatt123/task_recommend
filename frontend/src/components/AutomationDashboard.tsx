import React, { useState, useEffect } from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Switch } from '@/components/ui/switch';
import { 
  Bot, 
  Users, 
  TrendingUp, 
  Clock, 
  CheckCircle, 
  AlertTriangle,
  Settings,
  Play,
  Pause,
  RotateCcw
} from 'lucide-react';

interface AutomationStatus {
  enabled: boolean;
  total_assignments: number;
  success_rate: number;
  last_assignment: string;
  avg_assignment_time: number;
}

interface WorkloadStatus {
  total_employees: number;
  total_active_tasks: number;
  average_workload: number;
  available_for_assignment: number;
  workload_details: Array<{
    employee_code: string;
    employee_name: string;
    active_tasks: number;
    utilization_percent: number;
  }>;
}

interface AutomationRule {
  id: string;
  name: string;
  description: string;
  enabled: boolean;
  trigger_type: 'file_upload' | 'scheduled' | 'manual';
  assignment_strategy: 'ai_based' | 'workload_based' | 'round_robin';
  max_active_tasks: number;
  priority_level: 'LOW' | 'NORMAL' | 'HIGH';
}

export default function AutomationDashboard() {
  const [automationStatus, setAutomationStatus] = useState<AutomationStatus>({
    enabled: false,
    total_assignments: 0,
    success_rate: 0,
    last_assignment: '',
    avg_assignment_time: 0
  });

  const [workloadStatus, setWorkloadStatus] = useState<WorkloadStatus>({
    total_employees: 0,
    total_active_tasks: 0,
    average_workload: 0,
    available_for_assignment: 0,
    workload_details: []
  });

  const [automationRules, setAutomationRules] = useState<AutomationRule[]>([
    {
      id: '1',
      name: 'File Upload Auto-Assign',
      description: 'Automatically assign tasks when new files are uploaded',
      enabled: false,
      trigger_type: 'file_upload',
      assignment_strategy: 'ai_based',
      max_active_tasks: 5,
      priority_level: 'NORMAL'
    },
    {
      id: '2',
      name: 'Daily Workload Balance',
      description: 'Balance workload across team members daily at 9 AM',
      enabled: false,
      trigger_type: 'scheduled',
      assignment_strategy: 'workload_based',
      max_active_tasks: 4,
      priority_level: 'NORMAL'
    },
    {
      id: '3',
      name: 'Priority Task Assignment',
      description: 'Immediately assign high-priority tasks to best available employees',
      enabled: false,
      trigger_type: 'file_upload',
      assignment_strategy: 'ai_based',
      max_active_tasks: 3,
      priority_level: 'HIGH'
    }
  ]);

  const [isLoading, setIsLoading] = useState(false);

  // Load automation status
  useEffect(() => {
    loadAutomationStatus();
    loadWorkloadStatus();
    
    // Set up polling for real-time updates
    const interval = setInterval(() => {
      loadWorkloadStatus();
    }, 30000); // Update every 30 seconds

    return () => clearInterval(interval);
  }, []);

  const loadAutomationStatus = async () => {
    try {
      const response = await fetch('/api/v1/automation/status');
      if (response.ok) {
        const data = await response.json();
        setAutomationStatus(data);
      } else {
        console.error('Failed to load automation status');
      }
    } catch (error) {
      console.error('Failed to load automation status:', error);
    }
  };

  const loadWorkloadStatus = async () => {
    try {
      const response = await fetch('/api/v1/automation/workload-status');
      const data = await response.json();
      setWorkloadStatus(data);
    } catch (error) {
      console.error('Failed to load workload status:', error);
    }
  };

  const toggleAutomation = async (enabled: boolean) => {
    setIsLoading(true);
    try {
      // Call API to toggle automation
      setAutomationStatus(prev => ({ ...prev, enabled }));
    } catch (error) {
      console.error('Failed to toggle automation:', error);
    } finally {
      setIsLoading(false);
    }
  };

  const toggleRule = async (ruleId: string, enabled: boolean) => {
    try {
      // Call API to update rule
      setAutomationRules(prev => 
        prev.map(rule => 
          rule.id === ruleId ? { ...rule, enabled } : rule
        )
      );
    } catch (error) {
      console.error('Failed to update rule:', error);
    }
  };

  const triggerScan = async (scanType: string) => {
    setIsLoading(true);
    try {
      const response = await fetch('/api/v1/automation/trigger-scan', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ scan_type: scanType })
      });
      
      if (response.ok) {
        // Refresh data
        await loadWorkloadStatus();
        await loadAutomationStatus();
      }
    } catch (error) {
      console.error('Failed to trigger scan:', error);
    } finally {
      setIsLoading(false);
    }
  };

  const getUtilizationColor = (percent: number) => {
    if (percent >= 80) return 'text-red-600 bg-red-50';
    if (percent >= 60) return 'text-yellow-600 bg-yellow-50';
    return 'text-green-600 bg-green-50';
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold">Automation Dashboard</h1>
          <p className="text-muted-foreground">Manage automated task assignment workflows</p>
        </div>
        <div className="flex items-center gap-4">
          <div className="flex items-center gap-2">
            <Switch
              checked={automationStatus.enabled}
              onCheckedChange={toggleAutomation}
              disabled={isLoading}
            />
            <span className="text-sm font-medium">
              {automationStatus.enabled ? 'Enabled' : 'Disabled'}
            </span>
          </div>
        </div>
      </div>

      {/* Status Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Total Assignments</CardTitle>
            <Bot className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{automationStatus.total_assignments}</div>
            <p className="text-xs text-muted-foreground">
              Automated assignments
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Success Rate</CardTitle>
            <CheckCircle className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{automationStatus.success_rate}%</div>
            <p className="text-xs text-muted-foreground">
              Assignment success rate
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Avg Assignment Time</CardTitle>
            <Clock className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{automationStatus.avg_assignment_time}s</div>
            <p className="text-xs text-muted-foreground">
              Time to assign task
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Available Employees</CardTitle>
            <Users className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{workloadStatus.available_for_assignment}</div>
            <p className="text-xs text-muted-foreground">
              Ready for assignments
            </p>
          </CardContent>
        </Card>
      </div>

      {/* Main Content */}
      <Tabs defaultValue="rules" className="space-y-4">
        <TabsList>
          <TabsTrigger value="rules">Automation Rules</TabsTrigger>
          <TabsTrigger value="workload">Workload Status</TabsTrigger>
          <TabsTrigger value="actions">Quick Actions</TabsTrigger>
        </TabsList>

        <TabsContent value="rules" className="space-y-4">
          <div className="grid gap-4">
            {automationRules.map((rule) => (
              <Card key={rule.id}>
                <CardHeader>
                  <div className="flex items-center justify-between">
                    <div>
                      <CardTitle className="text-lg">{rule.name}</CardTitle>
                      <CardDescription>{rule.description}</CardDescription>
                    </div>
                    <Switch
                      checked={rule.enabled}
                      onCheckedChange={(enabled) => toggleRule(rule.id, enabled)}
                    />
                  </div>
                </CardHeader>
                <CardContent>
                  <div className="flex flex-wrap gap-2">
                    <Badge variant="outline">
                      {rule.trigger_type === 'file_upload' && 'File Upload'}
                      {rule.trigger_type === 'scheduled' && 'Scheduled'}
                      {rule.trigger_type === 'manual' && 'Manual'}
                    </Badge>
                    <Badge variant="outline">
                      {rule.assignment_strategy === 'ai_based' && 'AI Based'}
                      {rule.assignment_strategy === 'workload_based' && 'Workload Based'}
                      {rule.assignment_strategy === 'round_robin' && 'Round Robin'}
                    </Badge>
                    <Badge variant={rule.priority_level === 'HIGH' ? 'destructive' : 'secondary'}>
                      {rule.priority_level} Priority
                    </Badge>
                    <Badge variant="outline">
                      Max {rule.max_active_tasks} tasks
                    </Badge>
                  </div>
                </CardContent>
              </Card>
            ))}
          </div>
        </TabsContent>

        <TabsContent value="workload" className="space-y-4">
          <Card>
            <CardHeader>
              <CardTitle>Current Workload Distribution</CardTitle>
              <CardDescription>
                Real-time workload status across all employees
              </CardDescription>
            </CardHeader>
            <CardContent>
              <div className="space-y-4">
                <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                  <div className="text-center">
                    <div className="text-2xl font-bold">{workloadStatus.total_employees}</div>
                    <p className="text-sm text-muted-foreground">Total Employees</p>
                  </div>
                  <div className="text-center">
                    <div className="text-2xl font-bold">{workloadStatus.total_active_tasks}</div>
                    <p className="text-sm text-muted-foreground">Active Tasks</p>
                  </div>
                  <div className="text-center">
                    <div className="text-2xl font-bold">{workloadStatus.average_workload}</div>
                    <p className="text-sm text-muted-foreground">Avg Workload</p>
                  </div>
                </div>

                <div className="space-y-2">
                  {workloadStatus.workload_details.slice(0, 10).map((employee) => (
                    <div key={employee.employee_code} className="flex items-center justify-between p-3 border rounded-lg">
                      <div>
                        <p className="font-medium">{employee.employee_name}</p>
                        <p className="text-sm text-muted-foreground">#{employee.employee_code}</p>
                      </div>
                      <div className="flex items-center gap-2">
                        <span className="text-sm">{employee.active_tasks} active tasks</span>
                        <Badge className={getUtilizationColor(employee.utilization_percent)}>
                          {employee.utilization_percent}%
                        </Badge>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="actions" className="space-y-4">
          <div className="grid gap-4">
            <Card>
              <CardHeader>
                <CardTitle>Trigger Automation Actions</CardTitle>
                <CardDescription>
                  Manually trigger automation scans and assignments
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                  <Button
                    onClick={() => triggerScan('unassigned_files')}
                    disabled={isLoading}
                    className="w-full"
                  >
                    <Play className="h-4 w-4 mr-2" />
                    Assign Unassigned Files
                  </Button>
                  <Button
                    onClick={() => triggerScan('workload_rebalance')}
                    disabled={isLoading}
                    variant="outline"
                    className="w-full"
                  >
                    <TrendingUp className="h-4 w-4 mr-2" />
                    Check Workload Balance
                  </Button>
                  <Button
                    onClick={() => window.location.reload()}
                    variant="outline"
                    className="w-full"
                  >
                    <RotateCcw className="h-4 w-4 mr-2" />
                    Refresh Status
                  </Button>
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle>Automation Health</CardTitle>
                <CardDescription>
                  System health and performance metrics
                </CardDescription>
              </CardHeader>
              <CardContent>
                <div className="space-y-2">
                  <div className="flex items-center justify-between">
                    <span className="text-sm">API Response Time</span>
                    <Badge variant="outline">Normal</Badge>
                  </div>
                  <div className="flex items-center justify-between">
                    <span className="text-sm">Assignment Success Rate</span>
                    <Badge variant="outline">94.2%</Badge>
                  </div>
                  <div className="flex items-center justify-between">
                    <span className="text-sm">Employee Availability</span>
                    <Badge variant="outline">Good</Badge>
                  </div>
                  <div className="flex items-center justify-between">
                    <span className="text-sm">Last Sync</span>
                    <Badge variant="outline">2 mins ago</Badge>
                  </div>
                </div>
              </CardContent>
            </Card>
          </div>
        </TabsContent>
      </Tabs>
    </div>
  );
}

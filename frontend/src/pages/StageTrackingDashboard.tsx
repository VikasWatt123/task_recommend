import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Progress } from '@/components/ui/progress';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogDescription } from '@/components/ui/dialog';
import { 
  Clock, 
  AlertTriangle, 
  CheckCircle, 
  User, 
  FileText, 
  TrendingUp,
  Timer,
  Users,
  Activity,
  ArrowLeft,
  X
} from 'lucide-react';
import { getStageTrackingDashboard, manualSyncMongoToClickhouse, clearStageTrackingCache } from "@/lib/api";
import { toast } from "@/hooks/use-toast";
import { 
  FileStage, 
  PipelineData, 
  SLABreach, 
  StageConfig,
  StagesResponse
} from '@/types/stageTracking';
import { PermitFile } from '@/types';

const StageTrackingDashboard: React.FC = () => {
  const navigate = useNavigate();
  const [pipelineData, setPipelineData] = useState<PipelineData>({
    PRELIMS: [],
    PRODUCTION: [],
    COMPLETED: [],
    QC: [],
    DELIVERED: []
  });

  const isDebugEnabled = (() => {
    try {
      return localStorage.getItem('stageTrackingDebug') === 'true';
    } catch {
      return false;
    }
  })();
  const [slaBreaches, setSlaBreaches] = useState<SLABreach[]>([]);
  const [stageConfigs, setStageConfigs] = useState<StageConfig[]>([]);
  const [loading, setLoading] = useState(true);
  const [syncing, setSyncing] = useState(false);
  const [selectedStage, setSelectedStage] = useState<string>('all');
  const [permitFiles, setPermitFiles] = useState<PermitFile[]>([]);
  const [selectedStageFiles, setSelectedStageFiles] = useState<{stage: string, files: FileStage[]} | null>(null);
  const [metrics, setMetrics] = useState({
    totalFiles: 0,
    activeFiles: 0,
    completedFiles: 0,
    totalBreaches: 0,
    totalPenalties: 0
  });

  const showStageDetails = (stage: string, files: FileStage[]) => {
    if (isDebugEnabled) {
      console.log(`[Stage Tracking] Showing details for ${stage}:`, files.length, 'files');
    }
    setSelectedStageFiles({ stage, files });
  };

  useEffect(() => {
    fetchData(true); // Initial load with loading indicator
    const interval = setInterval(() => fetchData(false), 5000); // Silent refresh every 5 seconds
    return () => clearInterval(interval);
  }, []);

  const fetchData = async (showLoading = true, performManualSync = false) => {
    try {
      if (showLoading) setLoading(true);
      if (performManualSync) setSyncing(true);

      // Perform manual sync if requested
      if (performManualSync) {
        try {
          const syncResult = await manualSyncMongoToClickhouse();
          console.log('[Stage Tracking] Manual sync completed:', syncResult);
          
          // Show success toast
          toast({
            title: "Sync Completed",
            description: `Synced ${syncResult.synced_files} files, ${syncResult.breached_files} breaches detected`,
            variant: "default"
          });
        } catch (syncError) {
          console.error('[Stage Tracking] Manual sync failed:', syncError);
          
          // Show error toast
          toast({
            title: "Sync Failed",
            description: syncError instanceof Error ? syncError.message : "Unknown error occurred",
            variant: "destructive"
          });
        } finally {
          setSyncing(false);
        }
      }

      const dashboard = await getStageTrackingDashboard();
      const pipeline: PipelineData = dashboard?.data?.pipeline || { PRELIMS: [], PRODUCTION: [], COMPLETED: [], QC: [], DELIVERED: [] };
      const breaches: SLABreach[] = dashboard?.data?.sla_breaches || [];

      if (isDebugEnabled) {
        console.log('[Stage Tracking] Loaded pipeline from dashboard:', Object.keys(pipeline).map(k => `${k}: ${pipeline[k].length}`));
        console.log('[Stage Tracking] Loaded SLA breaches from dashboard:', breaches.length);
        
        // Check for duplicate file IDs across stages
        const allFileIds = [];
        const duplicates = [];
        
        Object.entries(pipeline).forEach(([stage, files]) => {
          files.forEach(file => {
            if (allFileIds.includes(file.file_id)) {
              duplicates.push({ file_id: file.file_id, stage, existing_in: allFileIds.indexOf(file.file_id) });
            } else {
              allFileIds.push(file.file_id);
            }
          });
        });
        
        if (duplicates.length > 0) {
          console.warn('[Stage Tracking] Found duplicate file IDs across stages:', duplicates);
        }
        
        // Additional debugging for React key collisions
        const allKeys = [];
        const keyCollisions = [];
        
        Object.entries(pipeline).forEach(([stage, files]) => {
          files.forEach(file => {
            const key = `${file.file_id}-${stage}-${file.current_assignment?.employee_name || 'unassigned'}-${file.current_status || 'unknown'}`;
            if (allKeys.includes(key)) {
              keyCollisions.push({ key, file_id: file.file_id, stage, existing_in: allKeys.indexOf(key) });
            } else {
              allKeys.push(key);
            }
          });
        });
        
        if (keyCollisions.length > 0) {
          console.error('[Stage Tracking] CRITICAL: Found React key collisions:', keyCollisions);
        } else {
          console.log('[Stage Tracking] ✅ All React keys are unique');
        }
      }

      setPipelineData(pipeline);
      setSlaBreaches(breaches);
      
      // Calculate metrics from backend data
      const allFiles = (Object.values(pipeline) as FileStage[][]).flat();
      const totalFiles = allFiles.length;
      const activeFiles = allFiles.filter(f => f.current_assignment && !['COMPLETED', 'DELIVERED'].includes(f.current_status)).length;
      const completedFiles = pipeline.COMPLETED.length;
      const totalBreaches = breaches.length;
      const totalPenalties = breaches.reduce((sum, b) => sum + Math.round((b.duration_minutes - b.max_minutes) / 60) * 10, 0);
      
      const newMetrics = {
        totalFiles,
        activeFiles,
        completedFiles,
        totalBreaches,
        totalPenalties
      };

      if (isDebugEnabled) {
        console.log('[Stage Tracking] Metrics:', newMetrics);
      }
      setMetrics(newMetrics);
      
      setStageConfigs([]); // Not available yet
      if (isDebugEnabled) {
        console.log('[Stage Tracking] Real data loaded');
      }
    } catch (error) {
      console.error('[Stage Tracking] Failed to fetch data:', error);
      // Set empty defaults on error to prevent infinite loading
      setPipelineData({ PRELIMS: [], PRODUCTION: [], COMPLETED: [], QC: [], DELIVERED: [] });
      setSlaBreaches([]);
      setStageConfigs([]);
      setPermitFiles([]);
    } finally {
      if (showLoading) setLoading(false);
    }
  };

  const getStageColor = (stage: string) => {
    const colors = {
      PRELIMS: 'bg-blue-100 text-blue-800 border-blue-200',
      PRODUCTION: 'bg-purple-100 text-purple-800 border-purple-200',
      COMPLETED: 'bg-gray-100 text-gray-800 border-gray-200',
      QC: 'bg-orange-100 text-orange-800 border-orange-200',
      DELIVERED: 'bg-green-100 text-green-800 border-green-200'
    };
    return colors[stage as keyof typeof colors] || 'bg-gray-100 text-gray-800';
  };

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'within_ideal': return 'text-green-600';
      case 'over_ideal': return 'text-yellow-600';
      case 'over_max': return 'text-red-600';
      case 'escalation_needed': return 'text-red-800 font-bold';
      default: return 'text-gray-600';
    }
  };

  const formatDuration = (minutes: number) => {
    if (minutes < 60) return `${minutes}m`;
    const hours = Math.floor(minutes / 60);
    const mins = minutes % 60;
    return `${hours}h ${mins}m`;
  };

  const getProgressPercentage = (duration: number, maxDuration: number) => {
    return Math.min((duration / maxDuration) * 100, 100);
  };

  const getProgressColor = (percentage: number, slaStatus?: string) => {
    if (slaStatus === 'escalation_needed') return 'bg-red-500';
    if (percentage > 100) return 'bg-red-500';
    if (percentage > 80) return 'bg-orange-500';
    if (percentage > 60) return 'bg-yellow-500';
    return 'bg-green-500';
  };

  const renderFileCard = (file: FileStage, stage: string) => {
    // Create a truly unique key combining file_id, stage, and a timestamp to prevent any collisions
    const uniqueKey = `${file.file_id}-${stage}-${file.current_assignment?.employee_name || 'unassigned'}-${file.current_status || 'unknown'}`;
    
    const stageConfig = stageConfigs.find(c => c.stage === stage);
    const duration = file.current_assignment?.duration_minutes || 0;
    // Use assignment's max_minutes if available, otherwise fall back to stage config
    const idealMinutes = file.current_assignment?.ideal_minutes || stageConfig?.ideal_minutes || 30;
    const maxMinutes = file.current_assignment?.max_minutes || stageConfig?.max_minutes || 60;
    const progressPercentage = getProgressPercentage(duration, maxMinutes);
    const slaStatus = file.current_assignment?.sla_status || 'within_ideal';
    
    return (
      <Card key={uniqueKey} className="mb-3">
        <CardContent className="p-4">
          <div className="flex justify-between items-start mb-2">
            <div>
              <h4 className="font-medium text-sm">{file.original_filename || file.file_id}</h4>
              {file.current_assignment && (
                <div className="flex items-center gap-2 mt-1">
                  <User className="w-3 h-3 text-gray-500" />
                  <span className="text-xs text-gray-600">{file.current_assignment.employee_name || 'Unassigned'}</span>
                </div>
              )}
            </div>
            <div className="flex flex-col items-end gap-1">
              <Badge className={getStageColor(stage)}>
                {stage}
              </Badge>
              {file.total_penalty_points > 0 && (
                <Badge variant="destructive" className="text-xs">
                  -{file.total_penalty_points} pts
                </Badge>
              )}
            </div>
          </div>

          {/* SLA Time Info */}
          <div className="grid grid-cols-3 gap-2 text-xs mb-2 bg-gray-50 p-2 rounded">
            <div>
              <p className="text-gray-500">Duration</p>
              <p className={`font-semibold ${getStatusColor(slaStatus)}`}>{formatDuration(duration)}</p>
            </div>
            <div>
              <p className="text-gray-500">Ideal Time</p>
              <p className="font-medium text-green-600">{formatDuration(idealMinutes)}</p>
            </div>
            <div>
              <p className="text-gray-500">Max Time</p>
              <p className="font-medium text-orange-600">{formatDuration(maxMinutes)}</p>
            </div>
          </div>

          {/* Progress Bar */}
          <div className="mt-2">
            <div className="flex justify-between text-xs text-gray-600 mb-1">
              <span className={getStatusColor(slaStatus)}>
                {slaStatus === 'within_ideal' ? '✓ On Track' : slaStatus === 'over_ideal' ? '⚠ Over Ideal' : '⚠ Over Max'}
              </span>
              <span>{Math.round(progressPercentage)}%</span>
            </div>
            <div className="relative">
              <Progress 
                value={Math.min(progressPercentage, 100)} 
                className="h-2"
              />
              <div 
                className="absolute top-0 left-0 h-2 rounded-full transition-all duration-300"
                style={{ 
                  width: `${Math.min(progressPercentage, 100)}%`,
                  backgroundColor: getProgressColor(progressPercentage, slaStatus)
                }}
              />
            </div>
          </div>

          <div className="flex justify-between items-center mt-2 text-xs text-gray-500">
            <span>Updated: {new Date(file.updated_at).toLocaleTimeString()}</span>
            {file.escalations_triggered > 0 && (
              <div className="flex items-center gap-1 text-red-600">
                <AlertTriangle className="w-3 h-3" />
                <span>{file.escalations_triggered} escalations</span>
              </div>
            )}
          </div>
        </CardContent>
      </Card>
    );
  };

  const renderStageColumn = (stage: string, files: FileStage[]) => {
    const stageConfig = stageConfigs.find(c => c.stage === stage);
    const stageBreaches = slaBreaches.filter(b => b.current_stage === stage);
    const fileMap = new Map<string, FileStage>();
    files.forEach((file) => {
      const existing = fileMap.get(file.file_id);
      if (!existing) {
        fileMap.set(file.file_id, file);
        return;
      }
      const existingTime = Date.parse(existing.updated_at || '') || 0;
      const candidateTime = Date.parse(file.updated_at || '') || 0;
      if (candidateTime >= existingTime) {
        fileMap.set(file.file_id, file);
      }
    });
    const dedupedFiles = Array.from(fileMap.values());
    
    return (
      <div key={stage} className="flex-1 min-w-0">
        <Card className="h-full">
          <CardHeader className="pb-3">
            <CardTitle className="text-lg flex items-center justify-between">
              <div className="flex items-center gap-2">
                <div className={`w-3 h-3 rounded-full ${getStageColor(stage).split(' ')[0]}`} />
                {stage}
              </div>
              <div className="flex items-center gap-2">
                <Badge variant="outline">{dedupedFiles.length}</Badge>
                {stageBreaches.length > 0 && (
                  <Badge variant="destructive">
                    <AlertTriangle className="w-3 h-3 mr-1" />
                    {stageBreaches.length}
                  </Badge>
                )}
              </div>
            </CardTitle>
            {stageConfig && (
              <div className="text-xs text-gray-600 space-y-1">
                <div>Ideal: {formatDuration(stageConfig.ideal_minutes)}</div>
                <div>Max: {formatDuration(stageConfig.max_minutes)}</div>
                <div>Escalate: {formatDuration(stageConfig.escalation_minutes)}</div>
              </div>
            )}
          </CardHeader>
          <CardContent className="pt-0">
            <div className="space-y-2 max-h-96 overflow-y-auto">
              {dedupedFiles.length === 0 ? (
                <div className="text-center text-gray-500 py-8">
                  <FileText className="w-8 h-8 mx-auto mb-2 opacity-50" />
                  <p className="text-sm">No files in this stage</p>
                </div>
              ) : (
                dedupedFiles.map(file => renderFileCard(file, stage))
              )}
            </div>
          </CardContent>
        </Card>
      </div>
    );
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex justify-between items-center">
        <div className="flex items-center gap-4">
          <Button 
            variant="outline" 
            size="sm" 
            onClick={() => navigate(-1)}
            className="flex items-center gap-2"
          >
            <ArrowLeft className="w-4 h-4" />
            Back
          </Button>
          <div>
            <h1 className="text-2xl font-bold">Stage Tracking Dashboard</h1>
            <p className="text-gray-600">Monitor file progress through workflow stages</p>
          </div>
        </div>
        <Button onClick={() => fetchData(true, true)} variant="outline" size="sm" disabled={syncing}>
          {syncing ? (
            <>
              <div className="w-4 h-4 mr-2 animate-spin rounded-full border-2 border-gray-300 border-t-blue-600"></div>
              Syncing...
            </>
          ) : (
            <>
              <Activity className="w-4 h-4 mr-2" />
              Refresh & Sync
            </>
          )}
        </Button>
      </div>

      {/* SLA Breaches Alert */}
      {slaBreaches.length > 0 && (
        <Alert className="border-red-200 bg-red-50">
          <AlertTriangle className="h-4 w-4 text-red-600" />
          <AlertDescription className="text-red-800">
            <strong>{slaBreaches.length} files</strong> have breached SLA thresholds and require attention.
            <Button 
              variant="link" 
              className="text-red-800 underline ml-2 p-0 h-auto"
              onClick={() => setSelectedStage('breaches')}
            >
              View Details
            </Button>
          </AlertDescription>
        </Alert>
      )}

      {/* Summary Cards */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <Card>
          <CardContent className="p-4">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-gray-600">Active Files</p>
                <p className="text-2xl font-bold">{metrics.activeFiles}</p>
              </div>
              <FileText className="w-8 h-8 text-blue-600 opacity-50" />
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardContent className="p-4">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-gray-600">Completed</p>
                <p className="text-2xl font-bold text-green-600">{metrics.completedFiles}</p>
              </div>
              <CheckCircle className="w-8 h-8 text-green-600 opacity-50" />
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardContent className="p-4">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-gray-600">SLA Breaches</p>
                <p className="text-2xl font-bold text-red-600">{slaBreaches.length}</p>
              </div>
              <AlertTriangle className="w-8 h-8 text-red-600 opacity-50" />
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardContent className="p-4">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-gray-600">Total Penalties</p>
                <p className="text-2xl font-bold text-orange-600">{metrics.totalPenalties}</p>
              </div>
              <TrendingUp className="w-8 h-8 text-orange-600 opacity-50" />
            </div>
          </CardContent>
        </Card>
      </div>

      {/* SLA Statistics Visualization */}
      <Card className="border-2 border-primary/20">
        <CardHeader className="pb-2">
          <CardTitle className="flex items-center gap-2">
            <Timer className="w-5 h-5 text-primary" />
            SLA Compliance Overview
          </CardTitle>
        </CardHeader>
        <CardContent>
          {(() => {
            const allFiles = Object.values(pipelineData).flat();
            const totalFiles = allFiles.length;
            const withinIdeal = allFiles.filter(f => f.current_assignment?.sla_status === 'within_ideal').length;
            const overIdeal = allFiles.filter(f => f.current_assignment?.sla_status === 'over_ideal').length;
            const overMax = allFiles.filter(f => f.current_assignment?.sla_status === 'over_max' || f.current_assignment?.sla_status === 'escalation_needed').length;
            const noAssignment = allFiles.filter(f => !f.current_assignment).length;
            
            const withinIdealPct = totalFiles > 0 ? Math.round((withinIdeal / totalFiles) * 100) : 0;
            const overIdealPct = totalFiles > 0 ? Math.round((overIdeal / totalFiles) * 100) : 0;
            const overMaxPct = totalFiles > 0 ? Math.round((overMax / totalFiles) * 100) : 0;
            
            return (
              <div className="space-y-4">
                {/* Visual Bar Chart */}
                <div className="flex h-8 rounded-lg overflow-hidden bg-gray-100">
                  {withinIdealPct > 0 && (
                    <div 
                      className="bg-green-500 flex items-center justify-center text-white text-xs font-medium transition-all"
                      style={{ width: `${withinIdealPct}%` }}
                    >
                      {withinIdealPct > 10 && `${withinIdealPct}%`}
                    </div>
                  )}
                  {overIdealPct > 0 && (
                    <div 
                      className="bg-yellow-500 flex items-center justify-center text-white text-xs font-medium transition-all"
                      style={{ width: `${overIdealPct}%` }}
                    >
                      {overIdealPct > 10 && `${overIdealPct}%`}
                    </div>
                  )}
                  {overMaxPct > 0 && (
                    <div 
                      className="bg-red-500 flex items-center justify-center text-white text-xs font-medium transition-all"
                      style={{ width: `${overMaxPct}%` }}
                    >
                      {overMaxPct > 10 && `${overMaxPct}%`}
                    </div>
                  )}
                </div>
                
                {/* Legend and Stats */}
                <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                  <div className="flex items-center gap-3 p-3 bg-green-50 rounded-lg border border-green-200">
                    <div className="w-4 h-4 rounded-full bg-green-500"></div>
                    <div>
                      <p className="text-xs text-gray-600">Within Ideal</p>
                      <p className="text-lg font-bold text-green-700">{withinIdeal}</p>
                    </div>
                  </div>
                  <div className="flex items-center gap-3 p-3 bg-yellow-50 rounded-lg border border-yellow-200">
                    <div className="w-4 h-4 rounded-full bg-yellow-500"></div>
                    <div>
                      <p className="text-xs text-gray-600">Over Ideal</p>
                      <p className="text-lg font-bold text-yellow-700">{overIdeal}</p>
                    </div>
                  </div>
                  <div className="flex items-center gap-3 p-3 bg-red-50 rounded-lg border border-red-200">
                    <div className="w-4 h-4 rounded-full bg-red-500"></div>
                    <div>
                      <p className="text-xs text-gray-600">Over Max / Breach</p>
                      <p className="text-lg font-bold text-red-700">{overMax + slaBreaches.length}</p>
                    </div>
                  </div>
                  <div className="flex items-center gap-3 p-3 bg-gray-50 rounded-lg border border-gray-200">
                    <div className="w-4 h-4 rounded-full bg-gray-400"></div>
                    <div>
                      <p className="text-xs text-gray-600">Unassigned</p>
                      <p className="text-lg font-bold text-gray-700">{noAssignment}</p>
                    </div>
                  </div>
                </div>
                
                {/* Stage Breakdown */}
                <div className="mt-4 pt-4 border-t">
                  <p className="text-sm font-medium text-gray-700 mb-3">By Stage</p>
                  <div className="grid grid-cols-5 gap-2">
                    {Object.entries(pipelineData).map(([stage, files]) => (
                      <div key={stage} className="text-center p-2 rounded-lg bg-gray-50">
                        <Badge className={getStageColor(stage)} variant="outline">
                          {stage}
                        </Badge>
                        <p 
                          className="text-2xl font-bold mt-1 cursor-pointer hover:text-primary transition-colors" 
                          onDoubleClick={() => showStageDetails(stage, files)}
                          title={`Double-click to view ${files.length} files in ${stage}`}
                        >
                          {files.length}
                        </p>
                        <p className="text-xs text-gray-500">files</p>
                      </div>
                    ))}
                  </div>
                </div>
              </div>
            );
          })()}
        </CardContent>
      </Card>

      {/* Pipeline View */}
      <Tabs value={selectedStage} onValueChange={setSelectedStage}>
        <TabsList className="grid w-full grid-cols-7">
          <TabsTrigger value="all">All Stages</TabsTrigger>
          <TabsTrigger value="PRELIMS">Prelims</TabsTrigger>
          <TabsTrigger value="PRODUCTION">Production</TabsTrigger>
          <TabsTrigger value="COMPLETED">Completed</TabsTrigger>
          <TabsTrigger value="QC">QC</TabsTrigger>
          <TabsTrigger value="DELIVERED">Delivered</TabsTrigger>
          <TabsTrigger value="breaches" className="text-red-600">
            SLA Breaches ({slaBreaches.length})
          </TabsTrigger>
        </TabsList>

        <TabsContent value="all" className="mt-6">
          <div className="grid grid-cols-1 lg:grid-cols-5 gap-4">
            {Object.entries(pipelineData).map(([stage, files]) => 
              renderStageColumn(stage, files)
            )}
          </div>
        </TabsContent>

        {Object.keys(pipelineData).map(stage => (
          <TabsContent key={stage} value={stage} className="mt-6">
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
              {renderStageColumn(stage, pipelineData[stage as keyof PipelineData])}
            </div>
          </TabsContent>
        ))}

        {/* SLA Breaches Tab */}
        <TabsContent value="breaches" className="mt-6">
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2 text-red-600">
                <AlertTriangle className="w-5 h-5" />
                SLA Breach Details ({slaBreaches.length} files)
              </CardTitle>
            </CardHeader>
            <CardContent>
              {slaBreaches.length === 0 ? (
                <div className="text-center py-8 text-gray-500">
                  <CheckCircle className="w-12 h-12 mx-auto mb-3 text-green-500" />
                  <p className="text-lg font-medium">No SLA breaches</p>
                  <p className="text-sm">All files are within SLA thresholds</p>
                </div>
              ) : (
                <div className="space-y-3">
                  {slaBreaches.map((breach, index) => (
                    <div key={index} className="border border-red-200 rounded-lg p-4 bg-red-50">
                      <div className="flex items-start justify-between">
                        <div className="flex-1">
                          <div className="flex items-center gap-2 mb-2">
                            <Badge className={getStageColor(breach.current_stage)}>
                              {breach.current_stage}
                            </Badge>
                            <span className="font-medium text-gray-900">
                              {breach.file_id}
                            </span>
                          </div>
                          <div className="grid grid-cols-2 gap-4 text-sm">
                            <div>
                              <p className="text-gray-600">Assigned to:</p>
                              <p className="font-medium flex items-center gap-1">
                                <User className="w-4 h-4" />
                                {breach.employee_name} ({breach.employee_code})
                              </p>
                            </div>
                            <div>
                              <p className="text-gray-600">Duration:</p>
                              <p className={`font-medium flex items-center gap-1 ${getStatusColor(breach.status)}`}>
                                <Clock className="w-4 h-4" />
                                {formatDuration(breach.duration_minutes)}
                              </p>
                            </div>
                            <div>
                              <p className="text-gray-600">Ideal Time:</p>
                              <p className="font-medium">{formatDuration(breach.ideal_minutes)}</p>
                            </div>
                            <div>
                              <p className="text-gray-600">Max Time:</p>
                              <p className="font-medium">{formatDuration(breach.max_minutes)}</p>
                            </div>
                          </div>
                          {breach.status === 'escalation_needed' && (
                            <div className="mt-3 p-2 bg-red-100 border border-red-300 rounded">
                              <p className="text-sm font-medium text-red-800 flex items-center gap-2">
                                <AlertTriangle className="w-4 h-4" />
                                Escalation Required - Exceeds maximum time limit
                              </p>
                            </div>
                          )}
                        </div>
                        <div className="ml-4">
                          <Progress 
                            value={getProgressPercentage(breach.duration_minutes, breach.max_minutes)} 
                            className="w-24 h-2"
                          />
                          <p className="text-xs text-center mt-1 text-gray-600">
                            {Math.round(getProgressPercentage(breach.duration_minutes, breach.max_minutes))}%
                          </p>
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
      
      {/* Stage Details Dialog */}
      <Dialog open={!!selectedStageFiles} onOpenChange={() => setSelectedStageFiles(null)}>
        <DialogContent className="max-w-4xl max-h-[80vh] overflow-y-auto">
          <DialogHeader>
            <DialogTitle className="flex items-center justify-between">
              Files in {selectedStageFiles?.stage} Stage
              <Button 
                variant="ghost" 
                size="sm" 
                onClick={() => setSelectedStageFiles(null)}
              >
                <X className="h-4 w-4" />
              </Button>
            </DialogTitle>
            <DialogDescription>
              View all {selectedStageFiles?.files.length} files currently in the {selectedStageFiles?.stage} stage
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-4">
            {selectedStageFiles?.files.map((file, idx) => (
              <Card key={idx} className="p-4">
                <div className="flex items-start justify-between">
                  <div className="flex-1">
                    <div className="flex items-center gap-2 mb-2">
                      <FileText className="h-4 w-4 text-primary" />
                      <span className="font-medium">{file.original_filename || file.file_id}</span>
                      <Badge className={getStageColor(file.current_status)}>
                        {file.current_status}
                      </Badge>
                    </div>
                    <div className="grid grid-cols-2 gap-4 text-sm">
                      <div>
                        <p className="text-gray-600">Created:</p>
                        <p>{new Date(file.created_at).toLocaleString()}</p>
                      </div>
                      <div>
                        <p className="text-gray-600">Updated:</p>
                        <p>{new Date(file.updated_at).toLocaleString()}</p>
                      </div>
                    </div>
                    {file.current_assignment && (
                      <div className="mt-2 flex items-center gap-2 text-sm">
                        <User className="h-4 w-4 text-gray-500" />
                        <span className="text-gray-600">Assigned to:</span>
                        <span className="font-medium">
                          {file.current_assignment.employee_name} ({file.current_assignment.employee_code})
                        </span>
                      </div>
                    )}
                  </div>
                </div>
              </Card>
            ))}
          </div>
        </DialogContent>
      </Dialog>
    </div>
  );
};

export default StageTrackingDashboard;

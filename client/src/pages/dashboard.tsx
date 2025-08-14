import { useQuery } from "@tanstack/react-query";
import { Sprout, Settings, Cpu } from "lucide-react";
import StatusCard from "@/components/status-card";
import MultiMetricChart from "@/components/multi-metric-chart";
import RecentReadings from "@/components/recent-readings";
import SystemInfo from "@/components/system-info";
import { Button } from "@/components/ui/button";
import { useToast } from "@/hooks/use-toast";
import { syncAntaresData } from "@/lib/api";
import type { SensorReading, SystemStatus } from "@shared/schema";

export default function Dashboard() {
  const { toast } = useToast();

  const { data: sensorReadings = [], isLoading: readingsLoading } = useQuery<
    SensorReading[]
  >({
    queryKey: ["/api/sensor-readings"],
    refetchInterval: 30000, // Refresh every minute
  });

  const { data: latestReading } = useQuery<SensorReading | null>({
    queryKey: ["/api/sensor-readings/latest"],
    refetchInterval: 30000, // Refresh every 30 seconds
  });

  const { data: systemStatus } = useQuery<SystemStatus>({
    queryKey: ["/api/system-status"],
    refetchInterval: 30000,
  });

  const handleSyncData = async () => {
    try {
      await syncAntaresData();
      toast({
        title: "Data synced successfully",
        description: "Latest sensor data has been retrieved from Antares.",
      });
    } catch (error) {
      toast({
        title: "Sync failed",
        description: "Failed to sync data from Antares API.",
        variant: "destructive",
      });
    }
  };

  const getOptimalityStatus = (value: number, min: number, max: number) => {
    if (value >= min && value <= max) return "optimal";
    if (Math.abs(value - min) < Math.abs(value - max)) return "low";
    return "high";
  };

  const getTemperatureStatus = (temp: number) =>
    getOptimalityStatus(temp, 22, 26);
  const getPhStatus = (ph: number) => getOptimalityStatus(ph, 5.5, 6.5);
  const getTdsLevelStatus = (tds: number) =>
    getOptimalityStatus(tds, 800, 1200);

  return (
    <div className="min-h-screen bg-slate-50">
      {/* Header */}
      <header className="bg-white shadow-sm border-b border-slate-200">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex justify-between items-center h-16">
            <div className="flex items-center space-x-4">
              <div className="flex items-center space-x-2">
                <Sprout className="h-6 w-6 text-green-600" />
                <h1 className="text-xl font-semibold text-slate-900">
                  HydroMonitor
                </h1>
              </div>
              <div className="hidden md:flex items-center space-x-2 text-sm text-slate-600">
                <div className="flex items-center space-x-1">
                  <div
                    className={`w-2 h-2 rounded-full ${
                      systemStatus?.connectionStatus === "connected"
                        ? "bg-green-500 animate-pulse"
                        : "bg-red-500"
                    }`}
                  />
                  <span>
                    {systemStatus?.connectionStatus === "connected"
                      ? "Connected to Antares"
                      : "Disconnected from Antares"}
                  </span>
                </div>
              </div>
            </div>

            <div className="flex items-center space-x-4">
              <Button
                variant="outline"
                size="sm"
                onClick={handleSyncData}
                className="text-green-600 border-green-600 hover:bg-green-50"
              >
                Sync Data
              </Button>
            </div>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6">
        {/* Status Overview */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8">
          <StatusCard
            title="Temperature"
            value={latestReading?.temperature ?? 0}
            unit="°C"
            icon="thermometer"
            trend={
              sensorReadings.length >= 2 && latestReading
                ? latestReading.temperature - sensorReadings[1].temperature
                : 0
            }
            status={
              latestReading
                ? getTemperatureStatus(latestReading.temperature)
                : "unknown"
            }
            optimalRange="22-26°C"
          />

          <StatusCard
            title="pH Level"
            value={latestReading?.ph ?? 0}
            unit=""
            icon="flask"
            trend={
              sensorReadings.length >= 2 && latestReading
                ? latestReading.ph - sensorReadings[1].ph
                : 0
            }
            status={latestReading ? getPhStatus(latestReading.ph) : "unknown"}
            optimalRange="5.5-6.5"
          />
          <StatusCard
            title="TDS Level"
            value={latestReading?.tdsLevel ?? 0}
            unit="ppm"
            icon="waves"
            trend={
              sensorReadings.length >= 2 && latestReading
                ? latestReading.tdsLevel - sensorReadings[1].tdsLevel
                : 0
            }
            status={
              latestReading
                ? getTdsLevelStatus(latestReading.tdsLevel)
                : "unknown"
            }
            optimalRange="800-1200 ppm"
          />
        </div>

        {/* Charts Section */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
          <MultiMetricChart data={sensorReadings} isLoading={readingsLoading} />
          <RecentReadings data={sensorReadings} isLoading={readingsLoading} />
        </div>

        {/* Detailed Data Section */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <SystemInfo systemStatus={systemStatus} />
        </div>
      </main>

      {/* Footer */}
      <footer className="bg-white border-t border-slate-200 mt-12">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6">
          <div className="flex flex-col sm:flex-row items-center justify-between space-y-2 sm:space-y-0">
            <div className="text-sm text-slate-600">
              © 2025 HydroMonitor - DRPTM Hydroponic System
            </div>
          </div>
        </div>
      </footer>
    </div>
  );
}

import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { useToast } from "@/hooks/use-toast";
import { updateAlertSettings } from "@/lib/api";
import { formatDistanceToNow } from "date-fns";
import type { SystemStatus, AlertSettings } from "@shared/schema";

interface SystemInfoProps {
  systemStatus?: SystemStatus;
}

export default function SystemInfo({ systemStatus }: SystemInfoProps) {
  const { toast } = useToast();
  const queryClient = useQueryClient();

  return (
    <Card className="bg-white rounded-xl shadow-sm border border-slate-200">
      <CardHeader>
        <CardTitle className="text-lg font-semibold text-slate-900">
          System Information
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="space-y-6">
          {/* Connection Status */}
          <div className="border-b border-slate-100 pb-4">
            <h4 className="text-sm font-medium text-slate-700 mb-3">
              Connection Status
            </h4>
            <div className="space-y-2">
              <div className="flex items-center justify-between">
                <span className="text-sm text-slate-600">Antares API</span>
                <div className="flex items-center space-x-2">
                  <div
                    className={`w-2 h-2 rounded-full ${
                      systemStatus?.connectionStatus === "connected"
                        ? "bg-green-500"
                        : "bg-red-500"
                    }`}
                  />
                  <span
                    className={`text-sm font-medium ${
                      systemStatus?.connectionStatus === "connected"
                        ? "text-green-600"
                        : "text-red-600"
                    }`}
                  >
                    {systemStatus?.connectionStatus === "connected"
                      ? "Connected"
                      : "Disconnected"}
                  </span>
                </div>
              </div>
              <div className="flex items-center justify-between">
                <span className="text-sm text-slate-600">Last Update</span>
                <span className="text-sm text-slate-900">
                  {systemStatus?.lastUpdate
                    ? formatDistanceToNow(new Date(systemStatus.lastUpdate), {
                        addSuffix: true,
                      })
                    : "Unknown"}
                </span>
              </div>
              <div className="flex items-center justify-between">
                <span className="text-sm text-slate-600">Data Points</span>
                <span className="text-sm text-slate-900">
                  {systemStatus?.dataPoints?.toLocaleString() ?? "0"}
                </span>
              </div>
            </div>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}

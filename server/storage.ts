// server/storage.ts
import { Pool } from "pg";
import {
  type SensorReading,
  type InsertSensorReading,
  type SystemStatus,
  type AlertSettings,
} from "@shared/schema";

export interface IStorage {
  // Sensor readings
  getSensorReadings(limit?: number): Promise<SensorReading[]>;
  getSensorReadingsByTimeRange(
    startTime: string,
    endTime: string
  ): Promise<SensorReading[]>;
  createSensorReading(reading: InsertSensorReading): Promise<SensorReading>;

  // System status
  getSystemStatus(): Promise<SystemStatus>;
  updateSystemStatus(status: Partial<SystemStatus>): Promise<SystemStatus>;

  // Alert settings
  getAlertSettings(): Promise<AlertSettings>;
  updateAlertSettings(settings: AlertSettings): Promise<AlertSettings>;

  // Connection management
  close(): Promise<void>;
}

export class PostgreSQLStorage implements IStorage {
  private pool: Pool;

  constructor(connectionString: string) {
    this.pool = new Pool({
      connectionString,
      ssl:
        process.env.NODE_ENV === "production"
          ? { rejectUnauthorized: false }
          : false,
      max: 10, // Maximum number of connections in the pool
      idleTimeoutMillis: 30000, // Close idle connections after 30 seconds
      connectionTimeoutMillis: 2000, // Return an error after 2 seconds if connection could not be established
    });

    // Handle pool errors
    this.pool.on("error", (err) => {
      console.error("Unexpected error on idle client", err);
    });
  }

  async testConnection(): Promise<void> {
    const client = await this.pool.connect();
    try {
      await client.query("SELECT NOW()");
      console.log("✅ PostgreSQL connection successful");
    } catch (error) {
      console.error("❌ PostgreSQL connection failed:", error);
      throw new Error(`Database connection failed: ${error}`);
    } finally {
      client.release();
    }
  }

  async initializeTables(): Promise<void> {
    const client = await this.pool.connect();
    try {
      // Create hydroponic_sensor_readings table
      await client.query(`
        CREATE TABLE IF NOT EXISTS hydroponic_sensor_readings (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
          temperature DECIMAL(5,2) NOT NULL CHECK (temperature >= -50 AND temperature <= 100),
          ph DECIMAL(4,2) NOT NULL CHECK (ph >= 0 AND ph <= 14),
          tds_level DECIMAL(7,2) NOT NULL CHECK (tds_level >= 0 AND tds_level <= 5000),
          created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
      `);

      // Create index for faster queries
      await client.query(`
        CREATE INDEX IF NOT EXISTS idx_hydroponic_sensor_timestamp 
        ON hydroponic_sensor_readings(timestamp DESC);
      `);

      // Create hydroponic_system_status table
      await client.query(`
        CREATE TABLE IF NOT EXISTS hydroponic_system_status (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          connection_status VARCHAR(20) NOT NULL DEFAULT 'disconnected' CHECK (connection_status IN ('connected', 'disconnected', 'error')),
          last_update TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
          data_points INTEGER DEFAULT 0 CHECK (data_points >= 0),
          cpu_usage DECIMAL(5,2) DEFAULT 0 CHECK (cpu_usage >= 0 AND cpu_usage <= 100),
          memory_usage DECIMAL(5,2) DEFAULT 0 CHECK (memory_usage >= 0 AND memory_usage <= 100),
          storage_usage DECIMAL(5,2) DEFAULT 0 CHECK (storage_usage >= 0 AND storage_usage <= 100),
          uptime VARCHAR(50) DEFAULT '0s',
          updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
      `);

      // Create hydroponic_alert_settings table
      await client.query(`
        CREATE TABLE IF NOT EXISTS hydroponic_alert_settings (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          temperature_alerts BOOLEAN DEFAULT true,
          ph_alerts BOOLEAN DEFAULT true,
          tds_level_alerts BOOLEAN DEFAULT true,
          updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
      `);

      // Insert default system status if not exists
      const statusExists = await client.query(`
        SELECT COUNT(*) as count FROM hydroponic_system_status;
      `);

      if (parseInt(statusExists.rows[0].count) === 0) {
        await client.query(`
          INSERT INTO hydroponic_system_status (connection_status, data_points, cpu_usage, memory_usage, storage_usage, uptime)
          VALUES ('connected', 0, 23, 30, 26, '0s');
        `);
      }

      // Insert default alert settings if not exists
      const alertExists = await client.query(`
        SELECT COUNT(*) as count FROM hydroponic_alert_settings;
      `);

      if (parseInt(alertExists.rows[0].count) === 0) {
        await client.query(`
          INSERT INTO hydroponic_alert_settings (temperature_alerts, ph_alerts, tds_level_alerts)
          VALUES (true, true, false);
        `);
      }

      console.log("✅ Hydroponic database tables initialized successfully");
    } catch (error) {
      console.error("❌ Error initializing tables:", error);
      throw error;
    } finally {
      client.release();
    }
  }

  async getSensorReadings(limit = 50): Promise<SensorReading[]> {
    const client = await this.pool.connect();
    try {
      const result = await client.query(
        `
        SELECT 
          id,
          timestamp,
          temperature,
          ph,
          tds_level as "tdsLevel",
          created_at as "createdAt"
        FROM hydroponic_sensor_readings
        ORDER BY timestamp DESC
        LIMIT $1
      `,
        [limit]
      );

      return result.rows.map((row) => ({
        ...row,
        timestamp: row.timestamp.toISOString(),
        createdAt: row.createdAt.toISOString(),
      }));
    } catch (error) {
      console.error("Error fetching sensor readings:", error);
      throw new Error(`Failed to fetch sensor readings: ${error}`);
    } finally {
      client.release();
    }
  }

  async getSensorReadingsByTimeRange(
    startTime: string,
    endTime: string
  ): Promise<SensorReading[]> {
    const client = await this.pool.connect();
    try {
      const result = await client.query(
        `
        SELECT 
          id,
          timestamp,
          temperature,
          ph,
          tds_level as "tdsLevel",
          created_at as "createdAt"
        FROM hydroponic_sensor_readings
        WHERE timestamp BETWEEN $1 AND $2
        ORDER BY timestamp ASC
      `,
        [startTime, endTime]
      );

      return result.rows.map((row) => ({
        ...row,
        timestamp: row.timestamp.toISOString(),
        createdAt: row.createdAt.toISOString(),
      }));
    } catch (error) {
      console.error("Error fetching sensor readings by range:", error);
      throw new Error(
        `Failed to fetch sensor readings by time range: ${error}`
      );
    } finally {
      client.release();
    }
  }

  async createSensorReading(
    insertReading: InsertSensorReading
  ): Promise<SensorReading> {
    const client = await this.pool.connect();
    try {
      // Validate input data
      if (insertReading.temperature < -50 || insertReading.temperature > 100) {
        throw new Error("Temperature must be between -50 and 100°C");
      }
      if (insertReading.ph < 0 || insertReading.ph > 14) {
        throw new Error("pH must be between 0 and 14");
      }
      if (insertReading.tdsLevel < 0 || insertReading.tdsLevel > 5000) {
        throw new Error("TDS Level must be between 0 and 5000 ppm");
      }

      const result = await client.query(
        `
        INSERT INTO hydroponic_sensor_readings (temperature, ph, tds_level)
        VALUES ($1, $2, $3)
        RETURNING 
          id,
          timestamp,
          temperature,
          ph,
          tds_level as "tdsLevel",
          created_at as "createdAt"
      `,
        [insertReading.temperature, insertReading.ph, insertReading.tdsLevel]
      );

      const row = result.rows[0];

      // Update data points count in system status
      await client.query(`
        UPDATE hydroponic_system_status
        SET 
          data_points = (SELECT COUNT(*) FROM hydroponic_sensor_readings),
          last_update = NOW(),
          updated_at = NOW()
        WHERE id IN (SELECT id FROM hydroponic_system_status ORDER BY updated_at DESC LIMIT 1)
      `);

      return {
        ...row,
        timestamp: row.timestamp.toISOString(),
        createdAt: row.createdAt.toISOString(),
      };
    } catch (error) {
      console.error("Error creating sensor reading:", error);
      throw new Error(`Failed to create sensor reading: ${error}`);
    } finally {
      client.release();
    }
  }

  async getSystemStatus(): Promise<SystemStatus> {
    const client = await this.pool.connect();
    try {
      const result = await client.query(`
        SELECT 
          id,
          connection_status as "connectionStatus",
          last_update as "lastUpdate",
          data_points as "dataPoints",
          cpu_usage as "cpuUsage",
          memory_usage as "memoryUsage",
          storage_usage as "storageUsage",
          uptime
        FROM hydroponic_system_status
        ORDER BY updated_at DESC
        LIMIT 1
      `);

      if (result.rows.length === 0) {
        throw new Error("No system status found in database");
      }

      const row = result.rows[0];
      return {
        ...row,
        lastUpdate: row.lastUpdate.toISOString(),
      };
    } catch (error) {
      console.error("Error fetching system status:", error);
      throw new Error(`Failed to fetch system status: ${error}`);
    } finally {
      client.release();
    }
  }

  async updateSystemStatus(
    status: Partial<SystemStatus>
  ): Promise<SystemStatus> {
    const client = await this.pool.connect();
    try {
      // Get current status first
      const currentResult = await client.query(`
        SELECT id FROM hydroponic_system_status ORDER BY updated_at DESC LIMIT 1
      `);

      if (currentResult.rows.length === 0) {
        throw new Error("No system status found in database");
      }

      const updates = [];
      const values = [];
      let paramIndex = 1;

      if (status.connectionStatus) {
        updates.push(`connection_status = $${paramIndex}`);
        values.push(status.connectionStatus);
        paramIndex++;
      }

      if (status.lastUpdate) {
        updates.push(`last_update = $${paramIndex}`);
        values.push(status.lastUpdate);
        paramIndex++;
      }

      if (status.dataPoints !== undefined) {
        updates.push(`data_points = $${paramIndex}`);
        values.push(status.dataPoints);
        paramIndex++;
      }

      if (status.cpuUsage !== undefined) {
        updates.push(`cpu_usage = $${paramIndex}`);
        values.push(status.cpuUsage);
        paramIndex++;
      }

      if (status.memoryUsage !== undefined) {
        updates.push(`memory_usage = $${paramIndex}`);
        values.push(status.memoryUsage);
        paramIndex++;
      }

      if (status.storageUsage !== undefined) {
        updates.push(`storage_usage = $${paramIndex}`);
        values.push(status.storageUsage);
        paramIndex++;
      }

      if (status.uptime) {
        updates.push(`uptime = $${paramIndex}`);
        values.push(status.uptime);
        paramIndex++;
      }

      updates.push(`updated_at = NOW()`);

      const result = await client.query(
        `
        UPDATE hydroponic_system_status
        SET ${updates.join(", ")}
        WHERE id = $${paramIndex}
        RETURNING 
          id,
          connection_status as "connectionStatus",
          last_update as "lastUpdate",
          data_points as "dataPoints",
          cpu_usage as "cpuUsage",
          memory_usage as "memoryUsage",
          storage_usage as "storageUsage",
          uptime
      `,
        [...values, currentResult.rows[0].id]
      );

      const row = result.rows[0];
      return {
        ...row,
        lastUpdate: row.lastUpdate.toISOString(),
      };
    } catch (error) {
      console.error("Error updating system status:", error);
      throw new Error(`Failed to update system status: ${error}`);
    } finally {
      client.release();
    }
  }

  async getAlertSettings(): Promise<AlertSettings> {
    const client = await this.pool.connect();
    try {
      const result = await client.query(`
        SELECT 
          temperature_alerts as "temperatureAlerts",
          ph_alerts as "phAlerts",
          tds_level_alerts as "tdsLevelAlerts"
        FROM hydroponic_alert_settings
        ORDER BY updated_at DESC
        LIMIT 1
      `);

      if (result.rows.length === 0) {
        // Create default settings if none exist
        const defaultSettings = {
          temperatureAlerts: true,
          phAlerts: true,
          tdsLevelAlerts: false,
        };

        await client.query(
          `
          INSERT INTO hydroponic_alert_settings (temperature_alerts, ph_alerts, tds_level_alerts)
          VALUES ($1, $2, $3)
        `,
          [
            defaultSettings.temperatureAlerts,
            defaultSettings.phAlerts,
            defaultSettings.tdsLevelAlerts,
          ]
        );

        return defaultSettings;
      }

      return result.rows[0];
    } catch (error) {
      console.error("Error fetching alert settings:", error);
      throw new Error(`Failed to fetch alert settings: ${error}`);
    } finally {
      client.release();
    }
  }

  async updateAlertSettings(settings: AlertSettings): Promise<AlertSettings> {
    const client = await this.pool.connect();
    try {
      // First, try to update existing settings
      const updateResult = await client.query(
        `
        UPDATE hydroponic_alert_settings
        SET 
          temperature_alerts = $1,
          ph_alerts = $2,
          tds_level_alerts = $3,
          updated_at = NOW()
        WHERE id IN (SELECT id FROM hydroponic_alert_settings ORDER BY updated_at DESC LIMIT 1)
        RETURNING 
          temperature_alerts as "temperatureAlerts",
          ph_alerts as "phAlerts",
          tds_level_alerts as "tdsLevelAlerts"
      `,
        [settings.temperatureAlerts, settings.phAlerts, settings.tdsLevelAlerts]
      );

      if (updateResult.rows.length > 0) {
        return updateResult.rows[0];
      }

      // If no rows were updated, insert new settings
      const insertResult = await client.query(
        `
        INSERT INTO hydroponic_alert_settings (temperature_alerts, ph_alerts, tds_level_alerts)
        VALUES ($1, $2, $3)
        RETURNING 
          temperature_alerts as "temperatureAlerts",
          ph_alerts as "phAlerts",
          tds_level_alerts as "tdsLevelAlerts"
      `,
        [settings.temperatureAlerts, settings.phAlerts, settings.tdsLevelAlerts]
      );

      return insertResult.rows[0];
    } catch (error) {
      console.error("Error updating alert settings:", error);
      throw new Error(`Failed to update alert settings: ${error}`);
    } finally {
      client.release();
    }
  }

  async close(): Promise<void> {
    await this.pool.end();
    console.log("PostgreSQL connection pool closed");
  }
}

// Create storage instance with validation
function createStorage(): PostgreSQLStorage {
  const databaseUrl = process.env.DATABASE_URL;

  if (!databaseUrl) {
    throw new Error(
      "DATABASE_URL environment variable is required. Please set it in your .env file.\n" +
        "Example: DATABASE_URL=postgresql://username:password@localhost:5432/database_name"
    );
  }

  console.log("🔄 Initializing PostgreSQL storage...");
  const storage = new PostgreSQLStorage(databaseUrl);

  // Test connection and initialize tables on startup
  storage
    .testConnection()
    .then(() => storage.initializeTables())
    .catch((error) => {
      console.error("💥 Failed to initialize database:", error);
      process.exit(1); // Exit if database is not available
    });

  return storage;
}

export const storage = createStorage();

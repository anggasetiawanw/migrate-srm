// === Interfaces ===

export interface Device {
  code: string;
  machineNumber: string;
}

export interface Checkpoint {
  deviceCode: string;
  lastTime: string; // ISO timestamp or "DONE"
}

export interface HistoryEntity {
  code: string;
  status: number;
  deviceTime: string;
  location?: string;
  plant?: string;
  createdAt?: Date;
}

export interface DeviceInnovate {
  id: string;
  code: string;
  machineNumber: string;
  location: string;
  plant: string;
  status: string;
  startDate: string;
  endDate: string;
  createdAt: string;
  createdBy: string;
  updatedAt: string;
  updatedBy: string;
}

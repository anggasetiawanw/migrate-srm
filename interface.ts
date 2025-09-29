
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

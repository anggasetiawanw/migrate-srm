import { deviceInnovatexList } from "./list_device";

const api_key =
  "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJlMTNlZmVhNi03N2EwLTQ5NzItOTg4OS01NmM1MjM5YzY4NjEiLCJlbWFpbCI6ImFkbWluQGxvY2FsLnRlc3QiLCJyb2xlIjoiYWRtaW4iLCJpYXQiOjE3Njc3NjM1OTIsImV4cCI6MTc2ODM2ODM5Mn0.GadQ-PnZRuTgx1iERvLFcoiN3ZrYoAGLxeO7tCJFYKs";
const base_url = "https://api-srm.rivvtech.com";

async function migrateDevice() {
  try {
    for (const device of deviceInnovatexList) {
      try {
        const payload = {
          machineNumber: device.code,
          startDate: "2024-09-08T07:00:00.000Z",
          endDate: "2024-09-08T17:00:00.000Z",
          plant: device.plant,
          location: device.location,
          latitude: -6.3274298,
          longitude: 107.1687231,
        };
        const response = await fetch(`${base_url}/devices`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Authorization: `Bearer ${api_key}`,
          },
          body: JSON.stringify(payload),
        });
      } catch (error) {
        console.error("Error migrating device:", error);
      }
    }
  } catch (error) {
    console.error("Unexpected error:", error);
  }
}

migrateDevice();

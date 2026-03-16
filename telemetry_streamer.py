import os
import json
import time
import random
import uuid
from datetime import datetime

LANDING_ZONE="/home/spark/landing_zone"
os.makedirs(LANDING_ZONE,exist_ok=True)

vehicles = [
    {"vehicle_id": f"AV-{str(uuid.uuid4())[:8]}","speed_kmh":60.0, "battery_kwh":100.0, "platoon_id":"Platoon_A"}
    for _ in range(3)
]+ [
    {"vehicle_id": f"AV-{str(uuid.uuid4())[:8]}","speed_kmh":45.0, "battery_kwh":85.0, "platoon_id":"Platoon_B"}
    for _ in range(2)
]
print("Starting Digital Twin Telemetry Streamer ....")
print(f"Broadcasting Cooperavtive Sensing data for {len(vehicles)} vehicles every 10 seconds.\n")

try:
    while True:
        batch_data=[]
        current_time = datetime.utcnow().isoformat()

        for v in vehicles:
            v["speed_kmh"]=max(0.0,min(120.0,v["speed_kmh"]+random.uniform(-5.0,5.0)))
            v["battery_kwh"]= max(0.0,v["battery_kwh"]-(v["speed_kmh"]*0.001))
            lidar_anomaly= random.random()<0.05

            payload={
                "vehicle_id": v["vehicle_id"],
                "timestamp":current_time,
                "speed_kmh":round(v["speed_kmh"],2),
                "battery_kwh":round(v["battery_kwh"],2),
                "lidar_anomaly_flag":lidar_anomaly,
                "cooperative_platoon_id":v["platoon_id"]
            }
            batch_data.append(payload)

        filename=f"telemetry_{int(time.time())}.json"
        filepath= os.path.join(LANDING_ZONE,filename)

        with open(filepath, 'w')as f:
            for record in batch_data:
                f.write(json.dumps(record)+'\n')

        print(f"[{current_time}] Streamed {len(batch_data)} vehicle states to {filename}")
        time.sleep(10)

except KeyboardInterrupt:
    print("\n Telemetry Streamer Stopped.")
# Load Test & Bottleneck Diagnostic Report
**Date:** 2026-08-15T03:24:44.840Z  
**Target:** `http://localhost:8080/logs`  
**Configuration:** Rate: 15000 logs/sec | Batch Size: 33 | Duration: 120s  

---

## 1. Summary
- **Attempted Logs:** 1800000 (14780.7 logs/sec)
- **Accepted Logs:** 1800000 (14780.7 logs/sec)
- **Rejected Logs:** 0
- **Success Rate:** 100.00%
- **Elapsed Time:** 121.78s

---

## 2. Bottleneck Diagnosis
- **Primary Bottleneck:** Express / App Container CPU Limit (0.5 CPU Throttling & Single-Thread Saturation)
- **Severity:** CRITICAL
- **Confidence:** HIGH

### Key Findings
- Top In-App CPU Consumer: 'Zod safeParse & Schema Validation' accounted for 46.5% of total Node.js CPU execution time.
- App Container CPU is SATURATED: Peak reached 45.2% (90.4% of its 0.5 CPU limit).

### Recommendations
1. [Express CPU Saturation] Increase app CPU limit in docker-compose.yml (e.g., from '0.5' to '1.0' or '2.0'). Express JSON body parsing and per-item Zod validation consume the full single-threaded CPU budget under high request volume.

---

## 3. Function-Level In-App CPU Breakdown
| Function / Pipeline Component | CPU % | Self Time (ms) |
| :--- | :--- | :--- |
| Zod safeParse & Schema Validation | 46.5% | 3,720 ms |
| JSON.parse (express.json body parser) | 23.8% | 1,904 ms |
| CSV Formatting (csvField) | 11.2% | 896 ms |
| JSON.stringify (attributes serialization) | 7.3% | 584 ms |
| Database Stream (pg-copy-streams) | 4.1% | 328 ms |
| batch.flatMap & Queue Management | 3.0% | 240 ms |
| Express Routing & Serialization | 2.4% | 192 ms |
| V8 Garbage Collection (GC) | 1.7% | 136 ms |

---

## 4. Latency Distribution
| Percentile | Latency |
| :--- | :--- |
| Min | 23.5ms |
| Avg | 93.1ms |
| p50 (Median) | 80.7ms |
| p90 | 149.8ms |
| p95 | 186.8ms |
| p99 | 296.7ms |
| Max | 585.6ms |

---

## 5. HTTP Status Breakdown
| Status Code / Category | Count | Percentage |
| :--- | :--- | :--- |
| 200 OK | 54546 | 100.0% |

---

## 6. Docker Container Metrics
| Container | Peak CPU | Peak CPU (% of limit) | Peak RAM | RAM Limit | Status |
| :--- | :--- | :--- | :--- | :--- | :--- |
| log-service-app | 45.2% | 90.4% | 53.7MB | 256MB | running |
| log-service-postgres | 12.5% | 12.5% | 176.2MB | 1024MB | running |

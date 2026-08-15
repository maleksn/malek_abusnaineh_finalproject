# Load Test & Bottleneck Diagnostic Report
**Date:** 2026-08-15T04:02:09.105Z  
**Target:** `http://localhost:8080/logs`  
**Configuration:** Rate: 15000 logs/sec | Batch Size: 500 | Duration: 10s  

---

## 1. Summary
- **Attempted Logs:** 150000 (14642.2 logs/sec)
- **Accepted Logs:** 150000 (14642.2 logs/sec)
- **Rejected Logs:** 0
- **Success Rate:** 100.00%
- **Elapsed Time:** 10.24s

---

## 2. Bottleneck Diagnosis
- **Primary Bottleneck:** None (System Handled Target Load Successfully)
- **Severity:** HEALTHY
- **Confidence:** HIGH

### Key Findings


### Recommendations


---

## 3. Function-Level In-App CPU Breakdown
| Function / Pipeline Component | CPU % | Self Time (ms) |
| :--- | :--- | :--- |


---

## 4. Latency Distribution
| Percentile | Latency |
| :--- | :--- |
| Min | 36.0ms |
| Avg | 102.7ms |
| p50 (Median) | 85.5ms |
| p90 | 179.6ms |
| p95 | 249.1ms |
| p99 | 363.7ms |
| Max | 397.4ms |

---

## 5. HTTP Status Breakdown
| Status Code / Category | Count | Percentage |
| :--- | :--- | :--- |
| 200 OK | 300 | 100.0% |

---

## 6. Docker Container Metrics
| Container | Peak CPU | Peak CPU (% of limit) | Peak RAM | RAM Limit | Status |
| :--- | :--- | :--- | :--- | :--- | :--- |


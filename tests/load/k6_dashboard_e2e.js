import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate } from 'k6/metrics';

// Custom metrics
const errorRate = new Rate('errors');

export const options = {
  stages: [
    { duration: '1m', target: 10 },   // Ramp up
    { duration: '2m', target: 10 },   // Stay at 10 users
    { duration: '1m', target: 0 },    // Ramp down
  ],
  thresholds: {
    http_req_duration: ['p(95)<2000'], // 95% of requests under 2s
    errors: ['rate<0.1'],              // Error rate under 10%
  },
};

const BASE_URL = 'http://localhost:8000';

export default function () {
  // Test API endpoints
  let response;
  
  // Test health endpoint
  response = http.get(`${BASE_URL}/health`);
  check(response, {
    'health check status is 200': (r) => r.status === 200,
    'health check response time < 500ms': (r) => r.timings.duration < 500,
  }) || errorRate.add(1);
  
  // Test latest KPIs
  response = http.get(`${BASE_URL}/kpis/latest`);
  check(response, {
    'latest KPIs status is 200 or 404': (r) => [200, 404].includes(r.status),
    'latest KPIs response time < 1000ms': (r) => r.timings.duration < 1000,
  }) || errorRate.add(1);
  
  // Test KPIs with parameters
  response = http.get(`${BASE_URL}/kpis?minutes_back=15&limit=50`);
  check(response, {
    'KPIs list status is 200': (r) => r.status === 200,
    'KPIs list response time < 1500ms': (r) => r.timings.duration < 1500,
    'KPIs list returns array': (r) => {
      try {
        const data = JSON.parse(r.body);
        return Array.isArray(data);
      } catch {
        return false;
      }
    },
  }) || errorRate.add(1);
  
  // Test funnel endpoint
  response = http.get(`${BASE_URL}/funnel?minutes_back=5`);
  check(response, {
    'funnel status is 200 or 404': (r) => [200, 404].includes(r.status),
    'funnel response time < 1000ms': (r) => r.timings.duration < 1000,
  }) || errorRate.add(1);
  
  // Test dashboard (if running)
  response = http.get('http://localhost:8501');
  check(response, {
    'dashboard accessible': (r) => r.status === 200,
  }); // Don't count as error if dashboard isn't running
  
  sleep(1);
}

# Updated: 2025-10-04 19:46:31
# Added during commit replay


# Updated: 2025-10-04 19:46:39
# Added during commit replay

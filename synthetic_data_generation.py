import random
import uuid
import json
import pandas as pd
from datetime import datetime, timedelta
import os

class SyntheticLogGenerator:
    def __init__(self):
        self.applications = [
            "AuthService", "EmailService", "NotificationService", 
            "UserManagement", "AuditService", "DatabaseService",
            "FileService", "ReportingService"
        ]
        
        self.success_patterns = [
            "Request processed successfully",
            "Operation completed without errors",
            "Transaction committed successfully", 
            "Data saved to database",
            "Response sent to client with status 200",
            "Process finished with status: SUCCESS",
            "Workflow step completed successfully",
            "Authentication successful for user",
            "File uploaded successfully",
            "Email sent successfully",
            "Report generated successfully",
            "Cache updated successfully",
            "Connection established successfully",
            "Session created successfully",
            "Validation passed for all fields",
            "Backup completed successfully",
            "Index rebuilt successfully",
            "Synchronization completed"
        ]
        
        self.error_patterns = [
            "Connection timeout after 30 seconds",
            "Database connection pool exhausted",
            "Authentication failed - invalid credentials",
            "Permission denied for user access",
            "Invalid request format - missing required fields",
            "Service temporarily unavailable",
            "Internal server error - null pointer exception",
            "Transaction rolled back due to constraint violation",
            "Validation failed - invalid email format",
            "File not found at specified path",
            "Disk space insufficient for operation",
            "Memory allocation failed",
            "Network unreachable",
            "SSL certificate expired",
            "Rate limit exceeded for API calls",
            "Deadlock detected in database",
            "Configuration file corrupted",
            "External service returned error 500"
        ]
        
        self.warning_patterns = [
            "Retry attempt #2 for failed operation",
            "Slow query detected - execution time > 5s",
            "Memory usage above 80% threshold",
            "Rate limit approaching - 90% of quota used",
            "Deprecated API endpoint used",
            "Connection pool running low",
            "Cache miss rate above normal threshold",
            "Large file size detected - may cause performance issues",
            "Session timeout approaching",
            "Backup taking longer than expected",
            "High CPU utilization detected",
            "Unusual traffic pattern detected"
        ]
        
        self.debug_patterns = [
            "Entering method with parameters",
            "SQL query executed",
            "Cache lookup performed",
            "Configuration loaded from file",
            "Thread started for background processing",
            "Serialization completed",
            "HTTP request headers logged",
            "Database connection acquired from pool"
        ]
    
    def generate_correlation_id(self):
        return f"CORR-{uuid.uuid4().hex[:12].upper()}"
    
    def generate_log_entry(self, correlation_id, app_name, timestamp, log_level="INFO"):
        
        if log_level == "ERROR":
            message = random.choice(self.error_patterns)
            details = f"Error in {app_name}: {message} | StackTrace: com.company.{app_name.lower()}.Exception"
        elif log_level == "WARN":
            message = random.choice(self.warning_patterns)
            details = f"Warning in {app_name}: {message} | Action: Monitor closely"
        elif log_level == "DEBUG":
            message = random.choice(self.debug_patterns)
            details = f"Debug {app_name}: {message} | Method: process_{random.choice(['request', 'data', 'transaction'])}"
        else:
            message = random.choice(self.success_patterns)
            details = f"{app_name}: {message} | Duration: {random.randint(10, 500)}ms"
        
        return {
            "timestamp": timestamp.isoformat(),
            "correlation_id": correlation_id,
            "application": app_name,
            "log_level": log_level,
            "message": message,
            "details": details,
            "thread_id": f"thread-{random.randint(1000, 9999)}",
            "user_id": f"user_{random.randint(100, 999)}",
            "session_id": f"session_{uuid.uuid4().hex[:8]}",
            "request_id": f"req_{uuid.uuid4().hex[:6]}",
            "host": f"server-{random.randint(1, 10)}.company.com",
            "method": random.choice(["GET", "POST", "PUT", "DELETE", "PATCH"]) if random.random() < 0.7 else None,
            "response_code": random.choice([200, 201, 400, 401, 403, 404, 500, 502, 503]) if random.random() < 0.6 else None
        }
    
    def generate_workflow_logs(self, correlation_id, workflow_type="email_send"):
        logs = []
        base_time = datetime.now() - timedelta(days=random.randint(0, 30))
        
        workflows = {
            "email_send": ["AuthService", "UserManagement", "EmailService", "AuditService"],
            "report_generation": ["AuthService", "DatabaseService", "ReportingService", "FileService", "NotificationService"],
            "user_registration": ["UserManagement", "DatabaseService", "EmailService", "AuditService"],
            "file_upload": ["AuthService", "FileService", "DatabaseService", "NotificationService"]
        }
        
        apps_sequence = workflows.get(workflow_type, random.sample(self.applications, 4))
        
        has_error = random.random() < 0.2
        error_at_step = random.randint(1, len(apps_sequence) - 1) if has_error else -1
        
        for i, app in enumerate(apps_sequence):
            step_time = base_time + timedelta(seconds=i * random.randint(1, 5))
            
            if i == error_at_step:
                log_level = "ERROR"
            elif random.random() < 0.1:  
                log_level = "WARN"
            else:
                log_level = "INFO"
            
            log_entry = self.generate_log_entry(correlation_id, app, step_time, log_level)
            logs.append(log_entry)
            
            if random.random() < 0.3:  
                context_time = step_time + timedelta(milliseconds=random.randint(10, 500))
                context_log = self.generate_log_entry(correlation_id, app, context_time, "DEBUG")
                context_log["message"] = f"Processing step {i+1} for correlation {correlation_id}"
                logs.append(context_log)
        
        return logs
    
    def generate_dataset(self, num_workflows=100):
        all_logs = []
        correlation_ids = []
        
        workflow_types = ["email_send", "report_generation", "user_registration", "file_upload"]
        
        for _ in range(num_workflows):
            correlation_id = self.generate_correlation_id()
            correlation_ids.append(correlation_id)
            workflow_type = random.choice(workflow_types)
            
            workflow_logs = self.generate_workflow_logs(correlation_id, workflow_type)
            all_logs.extend(workflow_logs)
        
        return all_logs, correlation_ids
    
    def save_logs_by_application(self, logs, output_dir="synthetic_logs"):
        os.makedirs(output_dir, exist_ok=True)
        
        app_logs = {}
        for log in logs:
            app = log["application"]
            if app not in app_logs:
                app_logs[app] = []
            app_logs[app].append(log)
        
        for app, logs_list in app_logs.items():
            filename = f"{output_dir}/{app}_logs.jsonl"
            with open(filename, 'w') as f:
                for log in logs_list:
                    f.write(json.dumps(log) + '\n')
            print(f"Saved {len(logs_list)} logs for {app} to {filename}")
    
    def generate_additional_logs_per_app(self, target_logs_per_app=100):
        print(f"Generating additional logs to ensure {target_logs_per_app} logs per application...")
        
        all_logs = []
        correlation_ids = []
        
        for app in self.applications:
            app_logs = []
            app_correlation_ids = []
            
            workflows_needed = target_logs_per_app // 3  
            
            for _ in range(workflows_needed):
                correlation_id = self.generate_correlation_id()
                app_correlation_ids.append(correlation_id)
                
                workflow_logs = self.generate_targeted_workflow_logs(correlation_id, target_app=app)
                app_logs.extend(workflow_logs)
                all_logs.extend(workflow_logs)
            
            correlation_ids.extend(app_correlation_ids)
            print(f"Generated {len([log for log in app_logs if log['application'] == app])} logs for {app}")
        
        return all_logs, correlation_ids
    
    def generate_targeted_workflow_logs(self, correlation_id, target_app):
        logs = []
        base_time = datetime.now() - timedelta(days=random.randint(0, 30))
        
        other_apps = [app for app in self.applications if app != target_app]
        workflow_apps = [target_app] + random.sample(other_apps, random.randint(2, 4))
        random.shuffle(workflow_apps)  
        
        has_error = random.random() < 0.2
        error_at_step = random.randint(0, len(workflow_apps) - 1) if has_error else -1
        
        for i, app in enumerate(workflow_apps):
            step_time = base_time + timedelta(seconds=i * random.randint(1, 5))
            
            if i == error_at_step:
                log_level = "ERROR"
            elif random.random() < 0.15:
                log_level = "WARN"
            else:
                log_level = "INFO"
            
            log_entry = self.generate_log_entry(correlation_id, app, step_time, log_level)
            logs.append(log_entry)
            
            if random.random() < 0.3:
                context_time = step_time + timedelta(milliseconds=random.randint(10, 500))
                context_log = self.generate_log_entry(correlation_id, app, context_time, "DEBUG")
                context_log["message"] = f"Processing step {i+1} for correlation {correlation_id}"
                logs.append(context_log)
            
            if random.random() < 0.2:
                perf_time = step_time + timedelta(milliseconds=random.randint(100, 1000))
                perf_log = self.generate_log_entry(correlation_id, app, perf_time, "INFO")
                perf_log["message"] = f"Performance metrics logged"
                perf_log["details"] = f"{app}: Response time: {random.randint(50, 500)}ms, Memory: {random.randint(100, 800)}MB"
                logs.append(perf_log)
        
        return logs
    
    def create_sample_files(self, target_logs_per_app=100):
        print("Generating comprehensive synthetic log dataset...")
        
        logs, correlation_ids = self.generate_additional_logs_per_app(target_logs_per_app)
        
        print(f"Generated {len(logs)} total log entries for {len(set(correlation_ids))} unique workflows")
        
        app_counts = {}
        for log in logs:
            app = log['application']
            app_counts[app] = app_counts.get(app, 0) + 1
        
        print("\nLogs per application:")
        for app, count in sorted(app_counts.items()):
            print(f"  {app}: {count} logs")
        
        self.save_logs_by_application(logs)
        
        unique_correlation_ids = list(set(correlation_ids))
        with open("correlation_ids.txt", 'w') as f:
            for corr_id in unique_correlation_ids:
                f.write(corr_id + '\n')
        
        print(f"\nDataset generation complete!")
        print(f"Total unique correlation IDs: {len(unique_correlation_ids)}")
        return logs, unique_correlation_ids

if __name__ == "__main__":
    generator = SyntheticLogGenerator()
    logs, correlation_ids = generator.create_sample_files(target_logs_per_app=100)
    
    print("\nSample log entries:")
    for i, log in enumerate(logs[:3]):
        print(f"{i+1}. {log}")
    
    app_counts = {}
    for log in logs:
        app = log['application']
        app_counts[app] = app_counts.get(app, 0) + 1
    
    print(f"\nFinal Statistics:")
    print(f"Total logs generated: {len(logs)}")
    print(f"Total correlation IDs: {len(correlation_ids)}")
    print(f"Average logs per correlation ID: {len(logs) / len(correlation_ids):.1f}")
    
    min_logs = min(app_counts.values())
    max_logs = max(app_counts.values())
    print(f"Minimum logs per app: {min_logs}")
    print(f"Maximum logs per app: {max_logs}")
    
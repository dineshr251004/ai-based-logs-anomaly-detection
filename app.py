import os
import re
import json
import chromadb
import pandas as pd
from datetime import datetime
from collections import defaultdict
from typing import List, Dict, Any

from pydantic import BaseModel, ValidationError, Field
from sentence_transformers import SentenceTransformer

from langchain_community.llms import Ollama
from langchain.prompts import PromptTemplate


class LogEntry(BaseModel):
    application: str
    correlation_id: str
    log_level: str
    message: str
    details: str
    timestamp: str


class LogAnalysisRAG:
    def __init__(self, chroma_db_path="./chroma_db", model_name="all-MiniLM-L6-v2"):
        self.chroma_client = chromadb.PersistentClient(path=chroma_db_path)
        self.embedding_model = SentenceTransformer(model_name)
        try:
            self.collection = self.chroma_client.get_collection(name="application_logs")
            print("Loaded existing ChromaDB collection")
        except:
            self.collection = self.chroma_client.create_collection(
                name="application_logs",
                metadata={"description": "Application logs for error analysis"}
            )
            print("Created new ChromaDB collection")

        self.applications = [
            "AuthService", "EmailService", "NotificationService",
            "UserManagement", "AuditService", "DatabaseService",
            "FileService", "ReportingService"
        ]

        self.llm = Ollama(model="llama3:8b")
        self.prompt_template = PromptTemplate.from_template("""
You are a log analysis assistant. Analyze the following logs for application '{app_name}' with correlation ID '{correlation_id}'.
Determine whether the process was SUCCESSFUL or FAILED.

Logs:
{log_content}

Respond with exactly one line in the format:
STATUS: [SUCCESS or FAILURE] - REASON: [brief explanation]
""")

    def load_application_logs(self, logs_directory="synthetic_logs"):
        print("Loading application logs into ChromaDB...")
        total_logs = 0
        for app in self.applications:
            log_file = f"{logs_directory}/{app}_logs.jsonl"
            if not os.path.exists(log_file):
                print(f"Warning: {log_file} not found")
                continue

            app_logs = []
            with open(log_file, 'r') as f:
                for line_num, line in enumerate(f, 1):
                    try:
                        raw = json.loads(line.strip())
                        log_entry = LogEntry(**raw)
                        app_logs.append(log_entry)
                    except (json.JSONDecodeError, ValidationError) as e:
                        print(f"Error in {log_file}, line {line_num}: {e}")

            self._store_logs_in_chroma(app_logs, app)
            total_logs += len(app_logs)
            print(f"Loaded {len(app_logs)} logs for {app}")
        print(f"Total logs loaded: {total_logs}")

    def _store_logs_in_chroma(self, logs: List[LogEntry], application: str):
        documents = []
        metadatas = []
        ids = []
        for i, log in enumerate(logs):
            doc_text = f"""
Application: {log.application}
Level: {log.log_level}
Message: {log.message}
Details: {log.details}
Timestamp: {log.timestamp}
""".strip()
            documents.append(doc_text)
            metadatas.append({
                "application": log.application,
                "correlation_id": log.correlation_id,
                "log_level": log.log_level,
                "timestamp": log.timestamp,
                "message": log.message,
                "details": log.details
            })
            ids.append(f"{application}_{log.correlation_id}_{i}")
        if documents:
            self.collection.add(
                documents=documents,
                metadatas=metadatas,
                ids=ids
            )

    def retrieve_logs_by_correlation_id(self, correlation_id: str, top_k: int = 5) -> Dict[str, List[Dict]]:
        app_logs = defaultdict(list)
        results = self.collection.get(
            where={"correlation_id": correlation_id},
            include=["documents", "metadatas"]
        )
        for doc, metadata in zip(results['documents'], results['metadatas']):
            app_name = metadata['application']
            app_logs[app_name].append({
                'document': doc,
                'metadata': metadata
            })
        for app in app_logs:
            app_logs[app] = app_logs[app][:top_k]
        return dict(app_logs)

    def analyze_logs_with_llm(self, correlation_id: str, app_logs: Dict[str, List[Dict]]) -> Dict[str, str]:
        analysis_results = {}
        for app_name in self.applications:
            logs = app_logs.get(app_name, [])
            if not logs:
                analysis_results[app_name] = "NO_LOGS"
                continue
            context = self._prepare_context_for_llm(logs)
            prompt = self.prompt_template.format(
                app_name=app_name,
                correlation_id=correlation_id,
                log_content=context
            )
            response = self.llm.invoke(prompt).strip()
            match = re.search(r"STATUS:\s*(SUCCESS|FAILURE)", response, re.IGNORECASE)
            status = match.group(1).upper() if match else "UNKNOWN"
            analysis_results[app_name] = status
        return analysis_results

    def _prepare_context_for_llm(self, logs: List[Dict]) -> str:
        context = ""
        for log_data in logs:
            metadata = log_data['metadata']
            details = metadata['details']
            if len(details) > 200:
                details = details[:200] + "..."
            context += f"{metadata['timestamp']} - {metadata['log_level']}: {metadata['message']} ({details})\n"
        return context.strip()

    def process_correlation_id(self, correlation_id: str) -> Dict[str, str]:
        print(f"Processing correlation ID: {correlation_id}")
        app_logs = self.retrieve_logs_by_correlation_id(correlation_id)
        print(f"Retrieved logs from {len(app_logs)} applications")
        analysis_results = self.analyze_logs_with_llm(correlation_id, app_logs)
        return analysis_results

    def generate_csv_report(self, correlation_ids: List[str], output_file: str = "analysis_report.csv"):
        print(f"Generating report for {len(correlation_ids)} correlation IDs...")
        all_results = []
        for corr_id in correlation_ids:
            results = self.process_correlation_id(corr_id)
            row = {"correlation_id": corr_id}
            for app in self.applications:
                row[app] = results.get(app, "NO_LOGS")
            all_results.append(row)
        df = pd.DataFrame(all_results)
        df.to_csv(output_file, index=False)
        print(f"Report saved to {output_file}")
        return df


def main():
    rag_system = LogAnalysisRAG()
    rag_system.load_application_logs()

    with open("correlation_ids.txt", 'r') as f:
        correlation_ids = [line.strip() for line in f.readlines()]

    report_df = rag_system.generate_csv_report(correlation_ids)
    print("\nSample results:")
    print(report_df.head())


if __name__ == "__main__":
    main()

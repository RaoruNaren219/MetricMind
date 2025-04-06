import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from typing import Dict, List, Optional
from pathlib import Path

from metricmind.utils.config import get_settings
from metricmind.utils.logger import setup_logger

logger = setup_logger(__name__)

class BenchmarkDashboard:
    """Streamlit dashboard for visualizing benchmark results."""
    
    def __init__(self, settings=None):
        self.settings = settings or get_settings()
        self.logger = logger
        
    def render_header(self):
        """Render dashboard header."""
        st.title("MetricMind Benchmark Dashboard")
        st.markdown("""
        This dashboard visualizes the performance comparison between two Dremio instances
        using TPC-DS queries.
        """)
        
    def render_performance_comparison(self, results: Dict):
        """Render performance comparison charts."""
        st.header("Performance Comparison")
        
        # Extract data for visualization
        data = []
        for query_name, query_data in results["queries"].items():
            for instance in ["dremio1", "dremio2"]:
                if instance in query_data:
                    instance_data = query_data[instance]
                    data.append({
                        "query": query_name,
                        "instance": instance,
                        "execution_time": instance_data.get("avg_execution_time", 0),
                        "memory_usage": instance_data.get("avg_memory_usage", 0),
                        "cpu_usage": instance_data.get("avg_cpu_usage", 0),
                        "success_rate": instance_data.get("success_rate", 0) * 100
                    })
        
        if not data:
            st.warning("No performance data available")
            return
            
        df = pd.DataFrame(data)
        
        # Execution time comparison
        fig_time = px.bar(
            df,
            x="query",
            y="execution_time",
            color="instance",
            title="Execution Time Comparison",
            labels={"execution_time": "Average Execution Time (s)", "query": "Query"}
        )
        st.plotly_chart(fig_time, use_container_width=True)
        
        # Success rate comparison
        fig_success = px.bar(
            df,
            x="query",
            y="success_rate",
            color="instance",
            title="Success Rate Comparison",
            labels={"success_rate": "Success Rate (%)", "query": "Query"}
        )
        st.plotly_chart(fig_success, use_container_width=True)
        
    def render_resource_usage(self, results: Dict):
        """Render resource usage charts."""
        st.header("Resource Usage")
        
        # Extract resource metrics
        if "resource_metrics" not in results or not results["resource_metrics"]:
            st.warning("No resource metrics available")
            return
            
        metrics_df = pd.DataFrame(results["resource_metrics"])
        
        # CPU usage over time
        if "cpu_percent" in metrics_df.columns:
            fig_cpu = px.line(
                metrics_df,
                y="cpu_percent",
                title="CPU Usage Over Time",
                labels={"cpu_percent": "CPU Usage (%)", "index": "Time"}
            )
            st.plotly_chart(fig_cpu, use_container_width=True)
            
        # Memory usage over time
        if "memory_percent" in metrics_df.columns:
            fig_memory = px.line(
                metrics_df,
                y="memory_percent",
                title="Memory Usage Over Time",
                labels={"memory_percent": "Memory Usage (%)", "index": "Time"}
            )
            st.plotly_chart(fig_memory, use_container_width=True)
            
    def render_summary_metrics(self, results: Dict):
        """Render summary metrics."""
        st.header("Summary Metrics")
        
        if "metadata" not in results or "overall" not in results["metadata"]:
            st.warning("No summary metrics available")
            return
            
        overall = results["metadata"]["overall"]
        
        # Create metrics display
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric(
                "Average Performance Ratio",
                f"{overall.get('avg_performance_ratio', 0):.2f}x"
            )
            
        with col2:
            st.metric(
                "Median Performance Ratio",
                f"{overall.get('median_performance_ratio', 0):.2f}x"
            )
            
        with col3:
            st.metric(
                "Overall Success Rate",
                f"{overall.get('success_rate', 0) * 100:.1f}%"
            )
            
    def render_error_analysis(self, results: Dict):
        """Render error analysis."""
        st.header("Error Analysis")
        
        # Extract errors
        errors = []
        for query_name, query_data in results["queries"].items():
            for instance in ["dremio1", "dremio2"]:
                if instance in query_data:
                    error = query_data[instance].get("error")
                    if error:
                        errors.append({
                            "query": query_name,
                            "instance": instance,
                            "error": error
                        })
        
        if not errors:
            st.success("No errors encountered during benchmark")
            return
            
        # Display errors
        for error in errors:
            st.error(f"**{error['query']}** on **{error['instance']}**: {error['error']}")
            
    def render(self, results: Dict):
        """Render the complete dashboard."""
        self.render_header()
        
        # Create tabs for different sections
        tab1, tab2, tab3, tab4 = st.tabs([
            "Performance", "Resources", "Summary", "Errors"
        ])
        
        with tab1:
            self.render_performance_comparison(results)
            
        with tab2:
            self.render_resource_usage(results)
            
        with tab3:
            self.render_summary_metrics(results)
            
        with tab4:
            self.render_error_analysis(results) 
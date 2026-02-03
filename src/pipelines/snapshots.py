import pandas as pd
import plotly.graph_objects as go
from typing import Dict, Any
from src.pipelines.base_pipeline import BaseDataPipeline


class SnapshotsPipeline(BaseDataPipeline):
    """
    Pipeline for analyzing player state snapshots.

    Metrics include distance traveled and movement methods (Walking vs Flying).
    """

    def __init__(self):
        super().__init__(action_type="SESSION_SNAPSHOT", output_name="snapshots")

    def _feature_engineering(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Converts distance units to Kilometers and ensures numeric types.
        """
        if df.empty: return df

        # Convert distance to KM
        if 'totalDistanceCm' in df.columns:
            df['totalDistance_km'] = pd.to_numeric(df['totalDistanceCm'], errors='coerce').fillna(0) / 100000
        elif 'totalDistance_km' in df.columns:
            df['totalDistance_km'] = pd.to_numeric(df['totalDistance_km'], errors='coerce').fillna(0)

        # Ensure Fly Ratio is numeric
        if 'fly_ratio' in df.columns:
            df['fly_ratio'] = pd.to_numeric(df['fly_ratio'], errors='coerce').fillna(0)

        return df

    def _generate_visualization_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Visualizes Distance Traveled and Fly Ratio.
        """
        plots = {}
        print(f"--- [DEBUG] SNAPSHOTS PIPELINE: Processing {len(df)} rows ---")

        # Distance Traveled Histogram
        if 'totalDistance_km' in df.columns:
            dist_data = df['totalDistance_km'].tolist()

            # Filter negligible distances for cleaner graphs
            dist_data = [d for d in dist_data if d > 0.1]

            if dist_data:
                fig = go.Figure(data=[go.Histogram(
                    x=dist_data,
                    nbinsx=30,
                    marker_color='#2ecc71',
                    name='Distance (km)'
                )])
                fig.update_layout(
                    title="Player Distance Traveled Distribution (km)",
                    xaxis_title="Kilometers",
                    template="plotly_white"
                )
                plots['distance_dist'] = fig.to_json()

        # Fly Ratio Histogram
        if 'fly_ratio' in df.columns:
            fly_data = df['fly_ratio'].tolist()

            if fly_data:
                fig2 = go.Figure(data=[go.Histogram(
                    x=fly_data,
                    nbinsx=20,
                    marker_color='#9b59b6',
                    name='Fly Ratio'
                )])
                fig2.update_layout(
                    title="Fly Ratio Distribution (0=Walk, 1=Fly)",
                    xaxis_title="Fly Ratio",
                    yaxis_title="Count",
                    template="plotly_white"
                )
                plots['fly_ratio'] = fig2.to_json()

        return plots
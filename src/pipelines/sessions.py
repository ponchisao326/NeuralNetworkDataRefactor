import pandas as pd
import plotly.graph_objects as go
from typing import Dict, Any
from src.pipelines.base_pipeline import BaseDataPipeline


class SessionsPipeline(BaseDataPipeline):
    """
    Pipeline for player session analysis.
    """

    def __init__(self):
        # Adjust 'action_type' if your source uses 'PLAYER_LOGIN' or similar
        super().__init__(action_type="SESSION_END", output_name="sessions")

    def _feature_engineering(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Converts session duration from milliseconds to minutes.
        """
        if df.empty: return df

        if 'durationMs' in df.columns:
            df['duration_min'] = pd.to_numeric(df['durationMs'], errors='coerce').fillna(0) / 60000

        return df

    def _generate_visualization_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Visualizes the distribution of session lengths.
        """
        plots = {}
        print(f"--- [DEBUG] SESSIONS PIPELINE: Processing {len(df)} rows ---")

        # Session Duration Histogram
        if 'duration_min' in df.columns:
            data_clean = df[df['duration_min'] > 0]['duration_min'].tolist()

            if data_clean:
                fig = go.Figure(data=[go.Histogram(
                    x=data_clean,
                    nbinsx=15,
                    marker_color='#9b59b6',
                    name='Minutes'
                )])
                fig.update_layout(
                    title="Session Duration Distribution (Minutes)",
                    xaxis_title="Minutes",
                    template="plotly_white"
                )
                plots['session_duration'] = fig.to_json()

        return plots
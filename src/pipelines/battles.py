import pandas as pd
import re
import plotly.graph_objects as go
from typing import Dict, Any
from src.pipelines.base_pipeline import BaseDataPipeline


class BattlesPipeline(BaseDataPipeline):
    """
    Pipeline responsible for processing battle-related data.

    This pipeline cleans battle duration, parses results (WIN/LOSS),
    and generates visualizations for Win Rates and Battle Durations.
    """

    def __init__(self):
        super().__init__(action_type="BATTLE_END", output_name="battles")

    def _feature_engineering(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms raw data into a format suitable for analysis.

        Key transformations:
        - Converts duration from milliseconds to seconds.
        - Normalizes battle results (WIN/LOSS) to binary targets.
        - Fills missing opponent types.
        """
        if df.empty:
            return df

        # Convert duration to seconds for better readability
        if 'durationMs' in df.columns:
            df['durationMs'] = pd.to_numeric(df['durationMs'], errors='coerce').fillna(0)
            df['duration_sec'] = df['durationMs'] / 1000

        # Normalize result to binary target (1 for WIN, 0 for LOSS)
        if 'result' in df.columns:
            df['target'] = df['result'].astype(str).str.upper().apply(lambda x: 1 if x == 'WIN' else 0)

        # ensure opponentType is a string to prevent categorization errors
        if 'opponentType' in df.columns:
            df['opponentType'] = df['opponentType'].astype(str).fillna("Unknown")

        return df

    def _generate_visualization_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Generates Plotly JSON objects for the HTML report.

        Visualizations:
        1. Win Probability by Opponent (Bar Chart).
        2. Battle Duration Distribution (Histogram).
        """
        plots = {}

        print(f"--- [DEBUG] BATTLES PIPELINE: Processing {len(df)} rows ---")

        # Win Rate Visualization
        if 'opponentType' in df.columns and 'target' in df.columns:
            # Group by opponent and calculate mean win rate
            win_rates = df.groupby('opponentType')['target'].mean().reset_index()
            win_rates = win_rates.sort_values(by='target', ascending=False)

            # Convert to native Python lists for JSON serialization safety
            x_data = win_rates['opponentType'].tolist()
            y_data = win_rates['target'].tolist()

            print(f"[DEBUG] Win Rate Data (X): {x_data}")

            if x_data:
                fig = go.Figure()
                fig.add_trace(go.Bar(
                    x=x_data,
                    y=y_data,
                    name='Win Rate',
                    marker_color=['#2ecc71', '#3498db', '#e74c3c', '#9b59b6'][:len(x_data)],
                    text=[f"{val:.1%}" for val in y_data],
                    textposition='auto'
                ))

                fig.update_layout(
                    title="Win Probability by Opponent",
                    yaxis=dict(title='Win Rate', range=[0, 1.1]),
                    xaxis=dict(title='Opponent Type'),
                    template="plotly_white"
                )

                plots['win_rate'] = fig.to_json()

        # Duration Visualization
        if 'duration_sec' in df.columns:
            # Filter valid durations
            durations = df[df['duration_sec'] > 0]['duration_sec'].tolist()

            if durations:
                fig2 = go.Figure(data=[go.Histogram(
                    x=durations,
                    nbinsx=30,
                    marker_color='#e67e22',
                    name='Duration'
                )])
                fig2.update_layout(
                    title="Battle Duration Distribution (Seconds)",
                    xaxis_title="Seconds",
                    yaxis_title="Count",
                    template="plotly_white"
                )
                plots['duration'] = fig2.to_json()

        return plots
import pandas as pd
import plotly.graph_objects as go
from typing import Dict, Any
from src.pipelines.base_pipeline import BaseDataPipeline


class ReleasedPipeline(BaseDataPipeline):
    """
    Pipeline for analyzing released Pokémon.

    Helps understand which species are discarded most often
    and the quality (IVs) of discarded Pokémon.
    """

    def __init__(self):
        super().__init__(action_type="POKEMON_RELEASED", output_name="released")

    def _feature_engineering(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Ensures numeric types for IVs and levels.
        """
        if df.empty: return df

        # Handle missing string values
        if 'species' in df.columns:
            df['species'] = df['species'].astype(str).fillna("Unknown")

        # Parse IV Percentage
        if 'iv_percentage' in df.columns:
            df['iv_percentage'] = pd.to_numeric(df['iv_percentage'], errors='coerce').fillna(0)

        # Parse Level
        if 'level' in df.columns:
            df['level'] = pd.to_numeric(df['level'], errors='coerce').fillna(0)

        return df

    def _generate_visualization_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Visualizes Top Released Species and IV Distribution.
        """
        plots = {}
        print(f"--- [DEBUG] RELEASED PIPELINE: Processing {len(df)} rows ---")

        # Top 10 Released Species
        if 'species' in df.columns:
            counts = df['species'].value_counts().head(10)

            x_data = counts.index.tolist()
            y_data = counts.values.tolist()

            if x_data:
                fig = go.Figure(data=[go.Bar(
                    x=x_data,
                    y=y_data,
                    marker_color='#e74c3c',
                    text=y_data,
                    textposition='auto'
                )])
                fig.update_layout(
                    title="Top 10 Released Pokémon Species",
                    xaxis_title="Species",
                    yaxis_title="Count",
                    template="plotly_white"
                )
                plots['top_released'] = fig.to_json()

        # IV Distribution of Released Pokemon
        if 'iv_percentage' in df.columns:
            iv_data = df['iv_percentage'].tolist()

            if iv_data:
                fig2 = go.Figure(data=[go.Histogram(
                    x=iv_data,
                    nbinsx=20,
                    marker_color='#3498db',
                    name='IV %'
                )])
                fig2.update_layout(
                    title="IV Percentage Distribution of Released Pokémon",
                    xaxis_title="IV Percentage (0-100%)",
                    yaxis_title="Count",
                    template="plotly_white"
                )
                plots['iv_distribution'] = fig2.to_json()

        return plots
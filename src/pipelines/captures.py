import pandas as pd
import plotly.graph_objects as go
from typing import Dict, Any
from src.pipelines.base_pipeline import BaseDataPipeline


class CapturesPipeline(BaseDataPipeline):
    """
    Pipeline for analyzing Pokémon capture events.
    """

    def __init__(self):
        super().__init__(action_type="POKEMON_CAPTURED", output_name="captures")

    def _feature_engineering(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans capture data strings to ensure valid categories.
        """
        if df.empty: return df

        # Handle missing string values
        if 'pokemon' in df.columns:
            df['pokemon'] = df['pokemon'].astype(str).fillna("Unknown")
        if 'biome' in df.columns:
            df['biome'] = df['biome'].astype(str).fillna("Unknown")

        return df

    def _generate_visualization_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Generates capture insights: Top Species and Top Biomes.
        """
        plots = {}
        print(f"--- [DEBUG] CAPTURES PIPELINE: Processing {len(df)} rows ---")

        # Top 10 Captured Pokémon
        col_pokemon = 'pokemon' if 'pokemon' in df.columns else 'species'

        if col_pokemon in df.columns:
            counts = df[col_pokemon].value_counts().head(10)

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
                fig.update_layout(title="Top 10 Captured Pokémon", template="plotly_white")
                plots['top_captures'] = fig.to_json()

        # Captures by Biome
        if 'biome' in df.columns:
            counts = df['biome'].value_counts().head(10)
            x_data = counts.index.tolist()
            y_data = counts.values.tolist()

            if x_data:
                fig2 = go.Figure(data=[go.Bar(
                    x=x_data,
                    y=y_data,
                    marker_color='#2ecc71',
                    text=y_data,
                    textposition='auto'
                )])
                fig2.update_layout(title="Top 10 Biomes for Captures", template="plotly_white")
                plots['biome_dist'] = fig2.to_json()

        return plots
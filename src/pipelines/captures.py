import pandas as pd
import plotly.graph_objects as go
from typing import Dict, Any
from src.pipelines.base_pipeline import BaseDataPipeline

class CapturesPipeline(BaseDataPipeline):
    """
    Pipeline for analyzing Pokémon capture events.

    Calculates genetic quality (IVs) of captured Pokémon and identifies the
    most popular species caught by players.
    """

    def __init__(self):
        super().__init__(action_type="POKEMON_CAPTURED", output_name="captures")

    def _feature_engineering(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculates Total IVs, IV Percentage, and Shiny flags.

        Args:
            df (pd.DataFrame): Raw capture data.

        Returns:
            pd.DataFrame: DataFrame with calculated 'iv_percentage' and 'is_shiny'.
        """
        # Calculate IV Percentage if columns exist
        iv_cols = [c for c in df.columns if c.startswith('ivs.')]
        if iv_cols:
            # Fill NaN with 0 to ensure valid numeric operations
            df[iv_cols] = df[iv_cols].fillna(0)
            df['iv_total'] = df[iv_cols].sum(axis=1)
            # Max possible IV total is 186
            df['iv_percentage'] = (df['iv_total'] / 186) * 100

        # Create binary Shiny flag (1/0)
        if 'shiny' in df.columns:
            df['is_shiny'] = df['shiny'].apply(lambda x: 1 if x else 0)

        return df

    def _generate_visualization_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Generates visualizations for capture analysis.

        Visualizations:
        1. Distribution of Pokémon Quality (IV %).
        2. Top 10 Most Captured Pokémon.

        Args:
            df (pd.DataFrame): Processed capture data.

        Returns:
            Dict[str, Any]: JSON-serialized Plotly figures.
        """
        plots = {}

        # Visualization: Distribution of Pokémon Quality (IV %)
        if 'iv_percentage' in df.columns:
            # Convert to native list and drop NaNs for safe serialization
            clean_ivs = pd.to_numeric(df['iv_percentage'], errors='coerce').dropna().tolist()

            fig = go.Figure(data=[go.Histogram(
                x=clean_ivs,
                marker_color='purple',
                opacity=0.75,
                xbins=dict(start=0, end=100, size=5)
            )])
            fig.update_layout(
                title="Distribution of Pokémon Quality (IV %)",
                xaxis_title="IV Percentage (0-100)",
                yaxis_title="Count",
                bargap=0.1
            )
            plots['iv_dist'] = fig.to_json()

        # Visualization: Top 10 Most Captured Pokémon
        if 'species' in df.columns:
            # Count frequencies and select Top 10
            top_species = df['species'].value_counts().head(10)

            # Extract native lists for Plotly
            species_names = top_species.index.tolist()
            species_counts = top_species.values.tolist()

            fig2 = go.Figure(data=[go.Bar(
                x=species_counts,
                y=species_names,
                orientation='h',  # Horizontal bars
                marker=dict(color='teal')
            )])
            fig2.update_layout(
                title="Top 10 Most Captured Pokémon",
                xaxis_title="Capture Count",
                yaxis=dict(autorange="reversed")  # Invert axis so #1 is at the top
            )
            plots['top_captured'] = fig2.to_json()

        return plots
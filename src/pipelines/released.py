import pandas as pd
import plotly.graph_objects as go
from typing import Dict, Any
from src.pipelines.base_pipeline import BaseDataPipeline

class ReleasedPipeline(BaseDataPipeline):
    """
    Pipeline for analyzing Pokémon release events.

    Focuses on understanding player behavior regarding which Pokémon are discarded,
    analyzing the genetic quality (IVs) of released Pokémon and ownership duration.
    """

    def __init__(self):
        super().__init__(action_type="POKEMON_RELEASED", output_name="released")

    def _feature_engineering(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculates genetic quality metrics and ownership duration.

        Args:
            df (pd.DataFrame): Raw release data.

        Returns:
            pd.DataFrame: Augmented DataFrame with IV percentages and ownership time.
        """
        # Calculate IV Totals and Percentage
        iv_cols = [c for c in df.columns if c.startswith('ivs.')]
        if iv_cols:
            df[iv_cols] = df[iv_cols].fillna(0)
            df['iv_total'] = df[iv_cols].sum(axis=1)
            # Max stats: 6 stats * 31 IVs = 186
            df['iv_percentage'] = (df['iv_total'] / 186) * 100

        # Convert ownership time from milliseconds to minutes
        if 'timeHeldCalculated' in df.columns:
            df['minutes_owned'] = df['timeHeldCalculated'] / 60000

        return df

    def _generate_visualization_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Generates histograms for released Pokémon quality.

        Args:
            df (pd.DataFrame): Processed data.

        Returns:
            Dict[str, Any]: JSON-serialized Plotly figures.
        """
        plots = {}

        # Visualization: Distribution of Released IVs
        if 'iv_percentage' in df.columns:
            # Ensure native Python list of floats for serialization
            clean_ivs = pd.to_numeric(df['iv_percentage'], errors='coerce').dropna().tolist()

            fig = go.Figure(data=[go.Histogram(
                x=clean_ivs,
                marker_color='brown',
                xbins=dict(start=0, end=100, size=5)
            )])
            fig.update_layout(
                title="Distribution of Released Pokémon IVs (The 'Discarded')",
                xaxis_title="IV Percentage",
                yaxis_title="Frequency"
            )
            plots['released_ivs'] = fig.to_json()

        return plots
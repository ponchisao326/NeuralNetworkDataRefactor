import pandas as pd
import plotly.graph_objects as go
from typing import Dict, Any
from src.pipelines.base_pipeline import BaseDataPipeline

class DeathsPipeline(BaseDataPipeline):
    """
    Pipeline for analyzing player death events.

    Categorizes causes of death and analyzes the "deadliness" of specific biomes
    and player levels at the time of death.
    """

    def __init__(self):
        super().__init__(action_type="PLAYER_DEATH", output_name="deaths")

    def _feature_engineering(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Categorizes death causes and flags high-level deaths.

        Args:
            df (pd.DataFrame): Raw death log data.

        Returns:
            pd.DataFrame: Augmented DataFrame with categories (PvE, PvP, Gravity, etc.).
        """
        def categorize(cause):
            """Maps specific damage source strings to broad categories."""
            c = str(cause).lower()
            if 'fall' in c or 'kinetic' in c: return 'Gravity'
            if 'mob' in c or 'arrow' in c: return 'PvE'
            if 'player' in c: return 'PvP'
            if 'lava' in c or 'fire' in c: return 'Fire'
            if 'drown' in c: return 'Drowning'
            return 'Other'

        if 'cause' in df.columns:
            df['death_category'] = df['cause'].apply(categorize)

        if 'level' in df.columns:
            df['level'] = pd.to_numeric(df['level'], errors='coerce').fillna(0)
            df['is_high_level'] = df['level'] > 30

        return df

    def _generate_visualization_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Generates visualizations for mortality analysis.

        Visualizations:
        1. Main Causes of Death (Pie Chart).
        2. Top 5 Deadliest Biomes (Bar Chart).
        3. Player Level Distribution at Death (Histogram).

        Args:
            df (pd.DataFrame): Processed death data.

        Returns:
            Dict[str, Any]: JSON-serialized Plotly figures.
        """
        plots = {}

        # Visualization: Main Causes of Death
        if 'death_category' in df.columns:
            counts = df['death_category'].value_counts()
            fig = go.Figure(data=[go.Pie(
                labels=counts.index.tolist(),
                values=counts.values.tolist(),
                hole=0.3
            )])
            fig.update_layout(title="Main Causes of Death")
            plots['causes'] = fig.to_json()

        # Visualization: Deadliest Biomes (Top 5)
        if 'biome' in df.columns:
            top_biomes = df['biome'].value_counts().head(5)
            fig2 = go.Figure(data=[go.Bar(
                y=top_biomes.index.tolist(),
                x=top_biomes.values.tolist(),
                orientation='h',
                marker=dict(color='salmon')
            )])
            fig2.update_layout(
                title="Top 5 Deadliest Biomes",
                xaxis_title="Number of Deaths",
                yaxis=dict(autorange="reversed")  # Invert axis so #1 is at the top
            )
            plots['deadliest_biomes'] = fig2.to_json()

        # Visualization: Player Level Distribution
        if 'level' in df.columns:
            levels = pd.to_numeric(df['level'], errors='coerce').dropna().tolist()
            fig3 = go.Figure(data=[go.Histogram(
                x=levels,
                marker_color='red',
                nbinsx=20
            )])
            fig3.update_layout(
                title="Player Level at Death",
                xaxis_title="Level",
                yaxis_title="Frequency"
            )
            plots['level_dist'] = fig3.to_json()

        return plots
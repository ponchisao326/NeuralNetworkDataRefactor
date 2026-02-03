import pandas as pd
import plotly.graph_objects as go
from typing import Dict, Any
from src.pipelines.base_pipeline import BaseDataPipeline


class RaidsPipeline(BaseDataPipeline):
    """
    Pipeline for analyzing Raid interactions.

    Includes a critical fix for column swapping (World vs Biome) known in the data source.
    """

    def __init__(self):
        super().__init__(action_type="RAID_INTERACTION", output_name="raids")

    def _feature_engineering(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Fixes known data issues and prepares result targets.
        """
        if df.empty: return df

        # Corrects column swap between 'world' and 'biome'
        print("[INFO] Applying Critical Fix: Swapping World and Biome columns.")
        if 'world' in df.columns and 'biome' in df.columns:
            real_worlds = df['biome'].copy()
            real_biomes = df['world'].copy()
            df['world'] = real_worlds
            df['biome'] = real_biomes

        # Standardize 'result' column
        if 'result' in df.columns:
            df['result'] = df['result'].astype(str).fillna("UNKNOWN")

        return df

    def _generate_visualization_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Visualizes Raid outcomes (Wins vs Losses).
        """
        plots = {}
        print(f"--- [DEBUG] RAIDS PIPELINE: Processing {len(df)} rows ---")

        # Raid Win/Loss Ratio
        if 'result' in df.columns:
            counts = df['result'].value_counts()

            x_data = counts.index.tolist()
            y_data = counts.values.tolist()

            print(f"[DEBUG] Raid Results: {x_data}")

            if x_data:
                # Color coding based on result
                colors = []
                for res in x_data:
                    res_upper = res.upper()
                    if 'WIN' in res_upper or 'VICTORY' in res_upper:
                        colors.append('#2ecc71')  # Green
                    elif 'LOSS' in res_upper or 'DEFEAT' in res_upper:
                        colors.append('#e74c3c')  # Red
                    else:
                        colors.append('#95a5a6')  # Grey

                fig = go.Figure(data=[go.Bar(
                    x=x_data,
                    y=y_data,
                    marker_color=colors,
                    text=y_data,
                    textposition='auto'
                )])
                fig.update_layout(
                    title="Raid Outcomes (Win/Loss)",
                    xaxis_title="Result",
                    yaxis_title="Count",
                    template="plotly_white"
                )
                plots['raid_results'] = fig.to_json()

        return plots
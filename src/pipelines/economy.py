import pandas as pd
import plotly.graph_objects as go
from typing import Dict, Any
from src.pipelines.base_pipeline import BaseDataPipeline


class EconomyPipeline(BaseDataPipeline):
    """
    Pipeline for tracking server economy (GTS Transactions).
    """

    def __init__(self):
        super().__init__(action_type="GTS_TRANSACTION", output_name="economy")

    def _feature_engineering(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardizes transaction amounts to numeric format.
        """
        if df.empty: return df

        # Normalize 'price' or 'amount' columns
        if 'price' in df.columns:
            df['amount'] = pd.to_numeric(df['price'], errors='coerce').fillna(0)
        elif 'amount' in df.columns:
            df['amount'] = pd.to_numeric(df['amount'], errors='coerce').fillna(0)

        return df

    def _generate_visualization_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Visualizes economic activity: Price distribution and Total Volume.
        """
        plots = {}
        print(f"--- [DEBUG] ECONOMY PIPELINE: Processing {len(df)} rows ---")

        # Price Distribution Histogram
        if 'amount' in df.columns:
            # Filter positive transactions
            data_clean = df[df['amount'] > 0]['amount'].tolist()

            if data_clean:
                fig = go.Figure(data=[go.Histogram(
                    x=data_clean,
                    nbinsx=20,
                    marker_color='#f1c40f',
                    name='Prices'
                )])
                fig.update_layout(title="Transaction Value Distribution", template="plotly_white")
                plots['price_dist'] = fig.to_json()

        # Total Volume Indicator
        if 'amount' in df.columns:
            total = sum(df['amount'].tolist())
            fig2 = go.Figure(go.Indicator(
                mode="number",
                value=total,
                title={"text": "Total Economy Volume"}
            ))
            plots['total_volume'] = fig2.to_json()

        return plots
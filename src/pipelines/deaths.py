import pandas as pd
import plotly.express as px
from typing import Dict, Any
from src.pipelines.base_pipeline import BaseDataPipeline


class DeathsPipeline(BaseDataPipeline):
    def __init__(self):
        super().__init__(action_type="PLAYER_DEATH", output_name="deaths")

    def _feature_engineering(self, df: pd.DataFrame) -> pd.DataFrame:
        def categorize(cause):
            c = str(cause).lower()
            if 'fall' in c or 'kinetic' in c: return 'Gravity'
            if 'mob' in c or 'arrow' in c: return 'PvE'
            if 'player' in c: return 'PvP'
            if 'lava' in c or 'fire' in c: return 'Fire'
            return 'Other'

        if 'cause' in df.columns:
            df['death_category'] = df['cause'].apply(categorize)

        if 'level' in df.columns:
            df['is_high_level'] = df['level'] > 30

        return df

    def _generate_visualization_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        plots = {}
        if 'death_category' in df.columns:
            fig = px.pie(df, names='death_category', title="Death Causes")
            plots['causes'] = fig.to_json()
        return plots
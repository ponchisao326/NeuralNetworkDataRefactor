import pandas as pd
import plotly.express as px
from typing import Dict, Any
from src.pipelines.base_pipeline import BaseDataPipeline

class CommandsPipeline(BaseDataPipeline):
    def __init__(self):
        super().__init__(action_type="COMMAND_USAGE", output_name="commands")

    def _feature_engineering(self, df: pd.DataFrame) -> pd.DataFrame:
        if 'command' in df.columns:
            # Extract base command
            df['base_command'] = df['command'].astype(str).apply(lambda x: x.split(' ')[0])
            # Length
            df['cmd_length'] = df['command'].astype(str).apply(len)
            # Teleport detection
            tp_kw = ['/home', '/warp', '/tpa', '/tpaccept', '/back', '/spawn', '/rtp']
            df['is_teleport'] = df['base_command'].isin(tp_kw)
        return df

    def _generate_visualization_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        plots = {}
        if 'base_command' in df.columns:
            top = df['base_command'].value_counts().head(10).reset_index()
            fig = px.bar(top, x='count', y='base_command', title="Top Commands")
            plots['top_cmds'] = fig.to_json()
        return plots
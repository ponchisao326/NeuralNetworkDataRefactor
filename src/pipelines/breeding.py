import pandas as pd
import json
import plotly.graph_objects as go
from typing import Dict, Any
from src.pipelines.base_pipeline import BaseDataPipeline


class BreedingPipeline(BaseDataPipeline):
    """
    Pipeline handling Pokémon breeding data analysis.

    Extracts deep nested JSON data for IVs (Individual Values) and
    analyzes breeding trends and genetic quality.
    """

    def __init__(self):
        super().__init__(action_type="POKEMON_BRED", output_name="breeding")

    def _clean_json_context(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Parses nested JSON structures within the 'context_data' column.

        Specific handling for breeding includes extracting species,
        shiny status, and detailed IV stats.
        """
        if df.empty: return df

        def parse_deep_context(row):
            try:
                # Parse JSON if string, otherwise use directly
                raw_data = row.get('context_data', '{}')
                data = json.loads(raw_data) if isinstance(raw_data, str) else raw_data

                if not isinstance(data, dict):
                    return pd.Series({})

                ivs = data.get('ivs', {})
                return pd.Series({
                    'species': data.get('species', 'unknown'),
                    'is_shiny': 1 if data.get('isShiny', False) else 0,
                    'iv_hp': ivs.get('PS_IV', 0),
                    'iv_atk': ivs.get('ATTACK_IV', 0),
                    'iv_def': ivs.get('DEFENCE_IV', 0),
                    'iv_spa': ivs.get('SP_ATTACK_IV', 0),
                    'iv_spd': ivs.get('SP_DEFENSE_IV', 0),
                    'iv_spe': ivs.get('SPEED_IV', 0)
                })
            except Exception:
                return pd.Series({})

        extracted = df.apply(parse_deep_context, axis=1)
        return pd.concat([df, extracted], axis=1)

    def _feature_engineering(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculates aggregate statistics for genetic quality (IVs).
        """
        if df.empty: return df

        # Ensure numeric types for IV columns
        iv_cols = ['iv_hp', 'iv_atk', 'iv_def', 'iv_spa', 'iv_spd', 'iv_spe']
        for col in iv_cols:
            if col not in df.columns:
                df[col] = 0
            else:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        # Calculate Total IV and Percentage
        df['iv_total'] = df[iv_cols].sum(axis=1)
        df['iv_percentage'] = (df['iv_total'] / 186) * 100

        return df

    def _generate_visualization_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Creates visualizations for breeding statistics.
        """
        plots = {}
        print(f"--- [DEBUG] BREEDING PIPELINE: Processing {len(df)} rows ---")

        # Top Bred Species Chart
        if 'species' in df.columns:
            counts = df['species'].value_counts().head(10)
            y_data = counts.index.tolist()
            x_data = counts.values.tolist()

            if x_data:
                fig = go.Figure(data=[go.Bar(
                    x=x_data,
                    y=y_data,
                    orientation='h',
                    marker_color='#8e44ad',
                    text=x_data,
                    textposition='auto'
                )])
                fig.update_layout(
                    title="Top 10 Bred Species",
                    xaxis_title="Count",
                    template="plotly_white",
                    yaxis={'categoryorder': 'total ascending'}
                )
                plots['top_bred'] = fig.to_json()

        # Genetic Quality Distribution
        if 'iv_percentage' in df.columns:
            iv_data = df['iv_percentage'].tolist()
            # Filter valid percentages
            iv_data = [x for x in iv_data if 0 <= x <= 100]

            print(f"[DEBUG] Valid IV Percentage points: {len(iv_data)}")

            if iv_data:
                fig2 = go.Figure(data=[go.Histogram(
                    x=iv_data,
                    nbinsx=20,
                    marker_color='#f39c12',
                    name='IV %'
                )])
                fig2.update_layout(
                    title="Genetic Quality Distribution (IV %)",
                    xaxis_title="IV Percentage (0-100%)",
                    yaxis_title="Count",
                    template="plotly_white",
                    xaxis=dict(range=[0, 100])
                )
                plots['iv_dist'] = fig2.to_json()

        return plots
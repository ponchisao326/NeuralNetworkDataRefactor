import pandas as pd
import plotly.graph_objects as go
import os
from typing import Dict, Any
from src.pipelines.base_pipeline import BaseDataPipeline
from src.config import Config
from src.connectors.api_client import APIClient


class SessionsPipeline(BaseDataPipeline):
    """
    Pipeline for analyzing player session lifecycles (Login/Logout).

    This pipeline consolidates login and logout events to calculate session durations
    and identify peak player activity hours.
    """

    def __init__(self):
        super().__init__(action_type="SESSION_LIFECYCLE", output_name="sessions")

    def _extract_data(self) -> pd.DataFrame:
        """
        Retrieves and merges LOGIN and LOGOUT datasets.

        Since session data consists of two distinct event types, this method
        fetches them separately (checking local cache first) and combines them
        into a single chronological event stream.

        Returns:
            pd.DataFrame: A combined DataFrame containing both LOGIN and LOGOUT events.
        """
        client = APIClient()

        # Load Login Data
        login_path = os.path.join(Config.RAW_DIR, "dataset_SESSION_LOGIN_raw.csv")
        if not os.path.exists(login_path):
            logins = pd.DataFrame(client.fetch_data("SESSION_LOGIN"))
            logins.to_csv(login_path, index=False)
        else:
            logins = pd.read_csv(login_path)

        # Load Logout Data
        logout_path = os.path.join(Config.RAW_DIR, "dataset_SESSION_LOGOUT_raw.csv")
        if not os.path.exists(logout_path):
            logouts = pd.DataFrame(client.fetch_data("SESSION_LOGOUT"))
            logouts.to_csv(logout_path, index=False)
        else:
            logouts = pd.read_csv(logout_path)

        # Tag events for identification
        if not logins.empty:
            logins['event_type'] = 'LOGIN'
        if not logouts.empty:
            logouts['event_type'] = 'LOGOUT'

        return pd.concat([logins, logouts], ignore_index=True)

    def _feature_engineering(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Reconstructs session durations by pairing login and logout events.

        Iterates through player events chronologically to match a LOGIN with its
        subsequent LOGOUT. It filters out invalid sessions (e.g., missing logouts
        or excessive durations).

        Args:
            df (pd.DataFrame): The combined raw event stream.

        Returns:
            pd.DataFrame: A DataFrame where each row represents a complete player session
            with calculated duration and temporal metadata.
        """
        if 'timestamp' in df.columns:
            df['dt'] = pd.to_datetime(df['timestamp'])

        # Sort by Player and Time to align events
        df = df.sort_values(by=['player_uuid', 'dt'])
        session_data = []

        for player, group in df.groupby('player_uuid'):
            group = group.sort_values('dt')
            login_dt = None

            for _, row in group.iterrows():
                if row['event_type'] == 'LOGIN':
                    login_dt = row['dt']
                elif row['event_type'] == 'LOGOUT' and login_dt is not None:
                    # Calculate duration in minutes
                    duration_min = (row['dt'] - login_dt).total_seconds() / 60

                    # Filter valid sessions (between 0 minutes and 24 hours)
                    if 0 < duration_min < 1440:
                        session_data.append({
                            'player_uuid': player,
                            'duration_minutes': duration_min,
                            'hour_of_day': login_dt.hour,
                            'day_of_week': login_dt.day_name()
                        })

                    # Reset login timestamp for the next session
                    login_dt = None

        return pd.DataFrame(session_data)

    def _generate_visualization_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Generates Plotly visualizations for session analysis.

        Visualizations:
        1. Session Duration Distribution (Histogram).
        2. Player Activity Heatmap (Day of Week vs. Hour of Day).

        Args:
            df (pd.DataFrame): The processed session data.

        Returns:
            Dict[str, Any]: JSON-serialized Plotly figures.
        """
        plots = {}
        if df.empty:
            return plots

        # Visualization: Session Duration Distribution
        if 'duration_minutes' in df.columns:
            durations = df['duration_minutes'].tolist()
            fig = go.Figure(data=[go.Histogram(
                x=durations,
                marker_color='teal',
                nbinsx=30
            )])
            fig.update_layout(
                title="Player Session Duration (Minutes)",
                xaxis_title="Minutes",
                yaxis_title="Count",
                xaxis=dict(range=[0, 180])  # Visually limit to 3 hours for clarity
            )
            plots['duration_dist'] = fig.to_json()

        # Visualization: Activity Heatmap (Day vs Hour)
        if 'day_of_week' in df.columns and 'hour_of_day' in df.columns:
            # Prepare matrix manually to ensure native Python types for serialization
            days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            pivot = df.pivot_table(
                index='day_of_week',
                columns='hour_of_day',
                values='player_uuid',
                aggfunc='count',
                fill_value=0
            )

            # Reindex to ensure correct day order and fill missing values
            pivot = pivot.reindex(days_order).fillna(0)

            # Ensure all 24 hours are present in the columns
            for h in range(24):
                if h not in pivot.columns:
                    pivot[h] = 0
            pivot = pivot[sorted(pivot.columns)]

            fig2 = go.Figure(data=go.Heatmap(
                z=pivot.values.tolist(),
                x=pivot.columns.tolist(),
                y=pivot.index.tolist(),
                colorscale='YlOrRd'
            ))
            fig2.update_layout(
                title="Player Activity Heatmap (Logins)",
                xaxis_title="Hour of Day",
                yaxis_title="Day of Week"
            )
            plots['heatmap'] = fig2.to_json()

        return plots
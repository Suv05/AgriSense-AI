import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import json
import warnings
warnings.filterwarnings('ignore')

# Configure Streamlit page
st.set_page_config(
    page_title="🌾 Agricultural Intelligence Dashboard",
    page_icon="🌾",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Set up the color palette - Professional agricultural theme
COLORS = {
    'primary': '#2E7D32',      # Deep Green
    'secondary': '#4CAF50',    # Medium Green  
    'accent': '#81C784',       # Light Green
    'warning': '#FF8F00',      # Amber
    'danger': '#D32F2F',       # Red
    'info': '#1976D2',         # Blue
    'background': '#F8F9FA',   # Light Gray
    'text': '#2C3E50',         # Dark Gray
    'success': '#388E3C'       # Success Green
}

# Custom CSS for styling
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        color: #2E7D32;
        text-align: center;
        margin-bottom: 2rem;
        font-weight: bold;
    }
    .kpi-card {
        background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
        padding: 1.5rem;
        border-radius: 10px;
        text-align: center;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        margin: 0.5rem;
    }
    .kpi-number {
        font-size: 2.5rem;
        font-weight: bold;
        color: #2E7D32;
    }
    .kpi-label {
        font-size: 1rem;
        color: #666;
        margin-top: 0.5rem;
    }
    .insight-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 1rem;
        border-radius: 8px;
        margin: 0.5rem 0;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
    }
    .sidebar .sidebar-content {
        background: linear-gradient(180deg, #2E7D32 0%, #4CAF50 100%);
    }
</style>
""", unsafe_allow_html=True)

@st.cache_data
def load_and_analyze_data():
    """Load and perform initial analysis of the agricultural dataset"""
    # Load the data
    url = "https://hebbkx1anhila5yf.public.blob.vercel-storage.com/bquxjob_701babfd_198d5093090-WcIT1GxNzZhS8rCbow6dMVX6JsNlKj.csv"
    df = pd.read_csv(url)
    
    # Data preprocessing
    df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
    df['lon'] = pd.to_numeric(df['lon'], errors='coerce')
    df['ndvi'] = pd.to_numeric(df['ndvi'], errors='coerce')
    df['evi'] = pd.to_numeric(df['evi'], errors='coerce')
    df['soil_moisture'] = pd.to_numeric(df['soil_moisture'], errors='coerce')
    df['wind_speed'] = pd.to_numeric(df['wind_speed'], errors='coerce')
    df['wind_deg'] = pd.to_numeric(df['wind_deg'], errors='coerce')
    
    # Parse irrigation probabilities
    def parse_irrigation_probs(prob_str):
        try:
            data = json.loads(prob_str)
            probs = data['predicted_irrigation_need_probs']
            return float([p['prob'] for p in probs if p['label'] == '1'][0])
        except:
            return 0.0
    
    df['irrigation_prob'] = df['predicted_irrigation_need_probs'].apply(parse_irrigation_probs)
    
    # Create risk categories
    df['yield_category'] = pd.cut(df['predicted_yield_tph'], 
                                 bins=[0, 5, 8, 12, float('inf')], 
                                 labels=['Low', 'Medium', 'High', 'Excellent'])
    
    df['disease_risk_category'] = pd.cut(df['predicted_disease_risk'], 
                                        bins=[-float('inf'), -0.5, 0, 0.5, float('inf')], 
                                        labels=['Very Low', 'Low', 'Medium', 'High'])
    
    return df

def create_kpi_cards(df):
    """Create KPI summary cards"""
    return {
        'Total Locations': len(df),
        'Avg Yield (TPH)': f"{df['predicted_yield_tph'].mean():.2f}",
        'High Risk Locations': len(df[df['predicted_disease_risk'] > 0]),
        'Irrigation Needed': len(df[df['irrigation_prob'] > 0.5])
    }

def generate_insights(df):
    """Generate key insights from the data"""
    insights = []
    
    # Yield insights
    avg_yield = df['predicted_yield_tph'].mean()
    high_yield_locations = df[df['predicted_yield_tph'] > avg_yield * 1.2]['location'].nunique()
    insights.append(f"🌾 {high_yield_locations} locations show exceptional yield potential (>20% above average)")
    
    # Risk insights
    high_risk_count = len(df[df['predicted_disease_risk'] > 0])
    risk_percentage = (high_risk_count / len(df)) * 100
    insights.append(f"⚠️ {risk_percentage:.1f}% of locations require immediate attention for disease prevention")
    
    # Irrigation insights
    irrigation_needed = len(df[df['irrigation_prob'] > 0.5])
    irrigation_percentage = (irrigation_needed / len(df)) * 100
    insights.append(f"💧 {irrigation_percentage:.1f}% of locations need irrigation optimization")
    
    # Environmental insights
    optimal_ndvi = df[df['predicted_yield_tph'] > avg_yield]['ndvi'].mean()
    insights.append(f"🌱 Optimal NDVI range for high yield: {optimal_ndvi:.3f} ± 0.05")
    
    # Location insights
    best_location = df.groupby('location')['predicted_yield_tph'].mean().idxmax()
    best_yield = df.groupby('location')['predicted_yield_tph'].mean().max()
    insights.append(f"🏆 Top performing location: {best_location} with {best_yield:.2f} TPH average yield")
    
    return insights

def home_page(df):
    """Create the home page with KPIs and insights"""
    st.markdown('<h1 class="main-header">🌾 Agricultural Intelligence Dashboard</h1>', unsafe_allow_html=True)
    
    st.markdown("""
    <div style="text-align: center; margin-bottom: 2rem; font-size: 1.2rem; color: #666;">
        Transform your agricultural data into actionable insights with our comprehensive analytics platform
    </div>
    """, unsafe_allow_html=True)
    
    # KPI Cards
    st.subheader("📊 Key Performance Indicators")
    kpis = create_kpi_cards(df)
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-number">{kpis['Total Locations']}</div>
            <div class="kpi-label">Total Locations</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-number">{kpis['Avg Yield (TPH)']}</div>
            <div class="kpi-label">Avg Yield (TPH)</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-number">{kpis['High Risk Locations']}</div>
            <div class="kpi-label">High Risk Locations</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-number">{kpis['Irrigation Needed']}</div>
            <div class="kpi-label">Irrigation Needed</div>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Key Insights
    st.subheader("🔍 Key Insights")
    insights = generate_insights(df)
    
    for insight in insights:
        st.markdown(f"""
        <div class="insight-card">
            {insight}
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("<br>", unsafe_allow_html=True)
    st.subheader("📈 Quick Statistics Overview")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Highest Yield Location", 
                 df.groupby('location')['predicted_yield_tph'].mean().idxmax(),
                 f"{df.groupby('location')['predicted_yield_tph'].mean().max():.2f} TPH")
    
    with col2:
        st.metric("Average Temperature", 
                 f"{df['weather_temp'].mean():.1f}°C",
                 f"Range: {df['weather_temp'].min():.1f}°C - {df['weather_temp'].max():.1f}°C")
    
    with col3:
        st.metric("Average NDVI", 
                 f"{df['ndvi'].mean():.3f}",
                 f"Vegetation Health Index")

def yield_analysis_page(df):
    """Create yield analysis dashboard page"""
    st.title("🌾 Crop Yield Analysis Dashboard")
    
    # Create yield distribution chart
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=('Yield Distribution', 'Yield by Location (Top 15)', 
                       'Yield Categories', 'Yield vs Environmental Factors'),
        specs=[[{"type": "histogram"}, {"type": "bar"}],
               [{"type": "pie"}, {"type": "scatter"}]]
    )
    
    # Histogram
    fig.add_trace(
        go.Histogram(x=df['predicted_yield_tph'], nbinsx=30, 
                    marker_color=COLORS['primary'], name='Yield Distribution'),
        row=1, col=1
    )
    
    # Top locations by yield
    top_locations = df.groupby('location')['predicted_yield_tph'].mean().sort_values(ascending=False).head(15)
    fig.add_trace(
        go.Bar(x=top_locations.index, y=top_locations.values,
               marker_color=COLORS['secondary'], name='Avg Yield by Location'),
        row=1, col=2
    )
    
    # Yield categories pie chart
    yield_counts = df['yield_category'].value_counts()
    fig.add_trace(
        go.Pie(labels=yield_counts.index, values=yield_counts.values,
               marker_colors=[COLORS['danger'], COLORS['warning'], COLORS['secondary'], COLORS['success']],
               name='Yield Categories'),
        row=2, col=1
    )
    
    # Yield vs NDVI scatter
    fig.add_trace(
        go.Scatter(x=df['ndvi'], y=df['predicted_yield_tph'],
                  mode='markers', marker=dict(color=COLORS['info'], size=6, opacity=0.6),
                  name='Yield vs NDVI'),
        row=2, col=2
    )
    
    fig.update_layout(
        height=800,
        showlegend=False,
        plot_bgcolor='white',
        paper_bgcolor=COLORS['background'],
        font=dict(color='#2F4F4F', size=12),
        title_font=dict(color='#2F4F4F', size=14)
    )
    
    fig.update_annotations(font=dict(color='#2F4F4F', size=14))
    fig.update_xaxes(title_font=dict(color='#2F4F4F'), tickfont=dict(color='#2F4F4F'))
    fig.update_yaxes(title_font=dict(color='#2F4F4F'), tickfont=dict(color='#2F4F4F'))
    
    st.plotly_chart(fig, use_container_width=True)
    
    st.subheader("📊 Yield Analysis Insights")
    col1, col2 = st.columns(2)
    
    with col1:
        st.info(f"**Average Yield:** {df['predicted_yield_tph'].mean():.2f} TPH")
        st.info(f"**Highest Yield:** {df['predicted_yield_tph'].max():.2f} TPH")
        st.info(f"**Yield Standard Deviation:** {df['predicted_yield_tph'].std():.2f}")
    
    with col2:
        excellent_count = len(df[df['yield_category'] == 'Excellent'])
        st.success(f"**Excellent Yield Locations:** {excellent_count}")
        st.warning(f"**Low Yield Locations:** {len(df[df['yield_category'] == 'Low'])}")
        st.info(f"**Total Locations Analyzed:** {len(df)}")

def risk_assessment_page(df):
    """Create risk assessment dashboard page"""
    st.title("⚠️ Agricultural Risk Assessment Dashboard")
    
    # Create risk assessment chart
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=('Disease Risk Distribution', 'Risk vs Yield Correlation',
                       'Environmental Risk Factors', 'Location Risk Heatmap'),
        specs=[[{"type": "histogram"}, {"type": "scatter"}],
               [{"type": "bar"}, {"type": "heatmap"}]]
    )
    
    # Disease risk histogram
    fig.add_trace(
        go.Histogram(x=df['predicted_disease_risk'], nbinsx=25,
                    marker_color=COLORS['danger'], name='Disease Risk'),
        row=1, col=1
    )
    
    # Risk vs Yield scatter
    colors = df['predicted_disease_risk'].apply(lambda x: COLORS['danger'] if x > 0 else COLORS['success'])
    fig.add_trace(
        go.Scatter(x=df['predicted_disease_risk'], y=df['predicted_yield_tph'],
                  mode='markers', 
                  marker=dict(color=colors, size=8, opacity=0.7),
                  name='Risk vs Yield'),
        row=1, col=2
    )
    
    # Environmental factors affecting risk
    env_factors = ['weather_temp', 'weather_humidity', 'soil_moisture', 'ndvi']
    correlations = [df[factor].corr(df['predicted_disease_risk']) for factor in env_factors]
    
    fig.add_trace(
        go.Bar(x=env_factors, y=correlations,
               marker_color=[COLORS['danger'] if c > 0 else COLORS['success'] for c in correlations],
               name='Risk Correlations'),
        row=2, col=1
    )
    
    # Location risk heatmap (top 20 locations)
    location_risk = df.groupby('location').agg({
        'predicted_disease_risk': 'mean',
        'predicted_yield_tph': 'mean',
        'irrigation_prob': 'mean'
    }).head(20)
    
    fig.add_trace(
        go.Heatmap(z=location_risk.values.T,
                  x=location_risk.index,
                  y=['Disease Risk', 'Yield', 'Irrigation Need'],
                  colorscale='RdYlGn_r',
                  name='Location Heatmap'),
        row=2, col=2
    )
    
    fig.update_layout(
        height=800,
        showlegend=False,
        plot_bgcolor='white',
        paper_bgcolor=COLORS['background'],
        font=dict(color='#2F4F4F', size=12),
        title_font=dict(color='#2F4F4F', size=14)
    )
    
    fig.update_annotations(font=dict(color='#2F4F4F', size=14))
    fig.update_xaxes(title_font=dict(color='#2F4F4F'), tickfont=dict(color='#2F4F4F'))
    fig.update_yaxes(title_font=dict(color='#2F4F4F'), tickfont=dict(color='#2F4F4F'))
    
    st.plotly_chart(fig, use_container_width=True)
    
    st.subheader("🚨 Risk Assessment Summary")
    high_risk_locations = len(df[df['predicted_disease_risk'] > 0])
    risk_percentage = (high_risk_locations / len(df)) * 100
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.error(f"**High Risk Locations:** {high_risk_locations}")
    with col2:
        st.warning(f"**Risk Percentage:** {risk_percentage:.1f}%")
    with col3:
        st.success(f"**Low Risk Locations:** {len(df) - high_risk_locations}")

def environmental_monitoring_page(df):
    """Create environmental monitoring dashboard page"""
    st.title("🌡️ Environmental Monitoring Dashboard")
    
    # Create environmental monitoring chart
    fig = make_subplots(
        rows=2, cols=3,
        subplot_titles=('Temperature Distribution', 'Humidity vs Soil Moisture', 'NDVI vs EVI',
                       'Wind Patterns', 'Pressure Distribution', 'Environmental Correlations'),
        specs=[[{"type": "histogram"}, {"type": "scatter"}, {"type": "scatter"}],
               [{"type": "scatter"}, {"type": "histogram"}, {"type": "heatmap"}]]
    )
    
    # Temperature distribution
    fig.add_trace(
        go.Histogram(x=df['weather_temp'], nbinsx=20,
                    marker_color=COLORS['warning'], name='Temperature'),
        row=1, col=1
    )
    
    # Humidity vs Soil Moisture
    fig.add_trace(
        go.Scatter(x=df['weather_humidity'], y=df['soil_moisture'],
                  mode='markers', marker=dict(color=COLORS['info'], size=6, opacity=0.6),
                  name='Humidity vs Soil'),
        row=1, col=2
    )
    
    # NDVI vs EVI
    fig.add_trace(
        go.Scatter(x=df['ndvi'], y=df['evi'],
                  mode='markers', marker=dict(color=COLORS['primary'], size=6, opacity=0.6),
                  name='NDVI vs EVI'),
        row=1, col=3
    )
    
    # Wind patterns
    fig.add_trace(
        go.Scatter(x=df['wind_deg'], y=df['wind_speed'],
                  mode='markers', marker=dict(color=COLORS['secondary'], size=6, opacity=0.6),
                  name='Wind Patterns'),
        row=2, col=1
    )
    
    # Pressure distribution
    fig.add_trace(
        go.Histogram(x=df['pressure'], nbinsx=20,
                    marker_color=COLORS['accent'], name='Pressure'),
        row=2, col=2
    )
    
    # Environmental correlations heatmap
    env_vars = ['weather_temp', 'weather_humidity', 'soil_moisture', 'ndvi', 'evi', 'wind_speed']
    corr_matrix = df[env_vars].corr()
    
    fig.add_trace(
        go.Heatmap(z=corr_matrix.values,
                  x=corr_matrix.columns,
                  y=corr_matrix.columns,
                  colorscale='RdBu',
                  zmid=0,
                  name='Correlations'),
        row=2, col=3
    )
    
    fig.update_layout(
        height=800,
        showlegend=False,
        plot_bgcolor='white',
        paper_bgcolor=COLORS['background'],
        font=dict(color='#2F4F4F', size=12),
        title_font=dict(color='#2F4F4F', size=14)
    )
    
    fig.update_annotations(font=dict(color='#2F4F4F', size=14))
    fig.update_xaxes(title_font=dict(color='#2F4F4F'), tickfont=dict(color='#2F4F4F'))
    fig.update_yaxes(title_font=dict(color='#2F4F4F'), tickfont=dict(color='#2F4F4F'))
    
    st.plotly_chart(fig, use_container_width=True)
    
    st.subheader("🌿 Environmental Conditions Summary")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Avg Temperature", f"{df['weather_temp'].mean():.1f}°C")
    with col2:
        st.metric("Avg Humidity", f"{df['weather_humidity'].mean():.1f}%")
    with col3:
        st.metric("Avg Soil Moisture", f"{df['soil_moisture'].mean():.3f}")
    with col4:
        st.metric("Avg NDVI", f"{df['ndvi'].mean():.3f}")

def geographic_analysis_page(df):
    """Create geographic analysis dashboard page"""
    st.title("🗺️ Geographic Analysis Dashboard")
    
    # Create maps
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Yield Distribution Map")
        fig_yield = px.scatter_mapbox(
            df, lat="lat", lon="lon", 
            color="predicted_yield_tph",
            size="predicted_yield_tph",
            hover_name="location",
            hover_data=["predicted_yield_tph"],
            color_continuous_scale="Viridis",
            size_max=15,
            zoom=6,
            mapbox_style="open-street-map"
        )
        fig_yield.update_layout(
            height=400,
            font=dict(color='#2F4F4F', size=12)
        )
        st.plotly_chart(fig_yield, use_container_width=True)
    
    with col2:
        st.subheader("Disease Risk Map")
        fig_risk = px.scatter_mapbox(
            df, lat="lat", lon="lon", 
            color="predicted_disease_risk",
            hover_name="location",
            hover_data=["predicted_disease_risk"],
            color_continuous_scale="RdYlGn_r",
            zoom=6,
            mapbox_style="open-street-map"
        )
        fig_risk.update_layout(
            height=400,
            font=dict(color='#2F4F4F', size=12)
        )
        st.plotly_chart(fig_risk, use_container_width=True)
    
    # Location performance summary
    st.subheader("📊 Location Performance Summary")
    location_summary = df.groupby('location').agg({
        'predicted_yield_tph': 'mean',
        'predicted_disease_risk': 'mean',
        'irrigation_prob': 'mean'
    }).sort_values('predicted_yield_tph', ascending=False).head(15)
    
    fig_bar = px.bar(
        x=location_summary.index, 
        y=location_summary['predicted_yield_tph'],
        color=location_summary['predicted_yield_tph'],
        color_continuous_scale="Viridis",
        title="Top 15 Locations by Average Yield"
    )
    fig_bar.update_layout(
        height=400,
        font=dict(color='#2F4F4F', size=12),
        title_font=dict(color='#2F4F4F', size=14)
    )
    fig_bar.update_xaxes(title_font=dict(color='#2F4F4F'), tickfont=dict(color='#2F4F4F'))
    fig_bar.update_yaxes(title_font=dict(color='#2F4F4F'), tickfont=dict(color='#2F4F4F'))
    st.plotly_chart(fig_bar, use_container_width=True)

def irrigation_optimization_page(df):
    """Create irrigation optimization dashboard page"""
    st.title("💧 Irrigation Optimization Dashboard")
    
    # Create irrigation optimization chart
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=('Irrigation Probability Distribution', 'Soil Moisture vs Irrigation Need',
                       'Weather Impact on Irrigation', 'Irrigation Efficiency Analysis'),
        specs=[[{"type": "histogram"}, {"type": "scatter"}],
               [{"type": "bar"}, {"type": "scatter"}]]
    )
    
    # Irrigation probability histogram
    fig.add_trace(
        go.Histogram(x=df['irrigation_prob'], nbinsx=20,
                    marker_color=COLORS['info'], name='Irrigation Probability'),
        row=1, col=1
    )
    
    # Soil moisture vs irrigation need
    fig.add_trace(
        go.Scatter(x=df['soil_moisture'], y=df['irrigation_prob'],
                  mode='markers', 
                  marker=dict(color=COLORS['info'], size=8, opacity=0.6),
                  name='Soil vs Irrigation'),
        row=1, col=2
    )
    
    # Weather impact on irrigation
    temp_bins = pd.cut(df['weather_temp'], bins=5, labels=['Very Cold', 'Cold', 'Moderate', 'Warm', 'Hot'])
    irrigation_by_temp = df.groupby(temp_bins)['irrigation_prob'].mean()
    
    fig.add_trace(
        go.Bar(x=irrigation_by_temp.index, y=irrigation_by_temp.values,
               marker_color=COLORS['warning'], name='Temperature Impact'),
        row=2, col=1
    )
    
    # Irrigation efficiency (irrigation need vs yield)
    colors = df['irrigation_prob'].apply(lambda x: COLORS['info'] if x > 0.5 else COLORS['success'])
    fig.add_trace(
        go.Scatter(x=df['irrigation_prob'], y=df['predicted_yield_tph'],
                  mode='markers',
                  marker=dict(color=colors, size=8, opacity=0.7),
                  name='Irrigation Efficiency'),
        row=2, col=2
    )
    
    fig.update_layout(
        height=800,
        showlegend=False,
        plot_bgcolor='white',
        paper_bgcolor=COLORS['background'],
        font=dict(color='#2F4F4F', size=12),
        title_font=dict(color='#2F4F4F', size=14)
    )
    
    fig.update_annotations(font=dict(color='#2F4F4F', size=14))
    fig.update_xaxes(title_font=dict(color='#2F4F4F'), tickfont=dict(color='#2F4F4F'))
    fig.update_yaxes(title_font=dict(color='#2F4F4F'), tickfont=dict(color='#2F4F4F'))
    
    st.plotly_chart(fig, use_container_width=True)
    
    st.subheader("💧 Irrigation Optimization Summary")
    irrigation_needed = len(df[df['irrigation_prob'] > 0.5])
    irrigation_percentage = (irrigation_needed / len(df)) * 100
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.info(f"**Locations Needing Irrigation:** {irrigation_needed}")
    with col2:
        st.warning(f"**Irrigation Percentage:** {irrigation_percentage:.1f}%")
    with col3:
        st.success(f"**Well-Watered Locations:** {len(df) - irrigation_needed}")

def main():
    """Main application function"""
    # Load data
    df = load_and_analyze_data()
    
    # Sidebar navigation
    st.sidebar.title("🌾 Navigation")
    st.sidebar.markdown("---")
    
    page = st.sidebar.selectbox(
        "Choose Dashboard",
        [
            "🏠 Home",
            "🌾 Yield Analysis", 
            "⚠️ Risk Assessment",
            "🌡️ Environmental Monitoring",
            "🗺️ Geographic Analysis",
            "💧 Irrigation Optimization"
        ]
    )
    
    st.sidebar.markdown("---")
    st.sidebar.info(f"**Dataset Info:**\n- Records: {len(df):,}\n- Locations: {df['location'].nunique()}\n- Features: {len(df.columns)}")
    
    # Route to appropriate page
    if page == "🏠 Home":
        home_page(df)
    elif page == "🌾 Yield Analysis":
        yield_analysis_page(df)
    elif page == "⚠️ Risk Assessment":
        risk_assessment_page(df)
    elif page == "🌡️ Environmental Monitoring":
        environmental_monitoring_page(df)
    elif page == "🗺️ Geographic Analysis":
        geographic_analysis_page(df)
    elif page == "💧 Irrigation Optimization":
        irrigation_optimization_page(df)

if __name__ == "__main__":
    main()

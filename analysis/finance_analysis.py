"""
Finance Data Analytics - Comprehensive Analysis
Generates all visualizations and insights from the medallion architecture data.
Run this script to generate all charts in the /images folder.
"""

import os
import sys
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Setup paths
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJECT_DIR, "tableau", "data")
IMG_DIR = os.path.join(PROJECT_DIR, "images")
os.makedirs(IMG_DIR, exist_ok=True)

# Style configuration
sns.set_palette("husl")
COLORS = {
    'primary': '#1f77b4',
    'secondary': '#ff7f0e',
    'success': '#2ca02c',
    'danger': '#d62728',
    'purple': '#9467bd',
    'teal': '#17becf',
    'bg': '#0e1117',
    'text': '#fafafa',
    'grid': '#2d3436',
}

def setup_dark_theme():
    """Apply dark theme for all charts."""
    plt.rcParams.update({
        'figure.facecolor': COLORS['bg'],
        'axes.facecolor': '#1a1d23',
        'text.color': COLORS['text'],
        'axes.labelcolor': COLORS['text'],
        'xtick.color': COLORS['text'],
        'ytick.color': COLORS['text'],
        'axes.edgecolor': COLORS['grid'],
        'grid.color': COLORS['grid'],
        'grid.alpha': 0.3,
        'font.size': 11,
        'axes.titlesize': 14,
        'axes.labelsize': 12,
        'figure.titlesize': 16,
    })

setup_dark_theme()


def load_data():
    """Load all CSV data files."""
    print("Loading data...")
    data = {}
    data['portfolio'] = pd.read_csv(os.path.join(DATA_DIR, "portfolio_performance.csv"), parse_dates=['TRADE_DATE'])
    data['crypto'] = pd.read_csv(os.path.join(DATA_DIR, "crypto_market.csv"), parse_dates=['TRADE_DATE'])
    data['forex'] = pd.read_csv(os.path.join(DATA_DIR, "forex_rates.csv"), parse_dates=['TRADE_DATE'])
    data['sector'] = pd.read_csv(os.path.join(DATA_DIR, "sector_performance.csv"), parse_dates=['TRADE_DATE'])
    data['news'] = pd.read_csv(os.path.join(DATA_DIR, "news_sentiment.csv"))
    data['summary'] = pd.read_csv(os.path.join(DATA_DIR, "executive_summary.csv"), parse_dates=['TRADE_DATE'])
    for k, v in data.items():
        print(f"  {k}: {len(v)} rows")
    return data


# ============================================================
# CHART 1: Stock Price Trends with Moving Averages
# ============================================================
def chart_stock_price_trends(data):
    """Multi-panel stock price chart with moving averages."""
    print("Generating: Stock Price Trends...")
    df = data['portfolio']
    tickers = df['TICKER'].unique()

    fig, axes = plt.subplots(2, 5, figsize=(24, 10))
    fig.suptitle('Stock Price Trends with Moving Averages (7D & 30D)', fontsize=18, fontweight='bold', color=COLORS['text'])

    for i, ticker in enumerate(tickers):
        ax = axes[i // 5][i % 5]
        stock = df[df['TICKER'] == ticker].sort_values('TRADE_DATE')

        ax.plot(stock['TRADE_DATE'], stock['CLOSE_PRICE'], color=COLORS['primary'], linewidth=2, label='Close')
        ax.plot(stock['TRADE_DATE'], stock['MOVING_AVG_7D'], color=COLORS['secondary'], linewidth=1, linestyle='--', label='7D MA')
        ax.plot(stock['TRADE_DATE'], stock['MOVING_AVG_30D'], color=COLORS['danger'], linewidth=1, linestyle=':', label='30D MA')

        ax.fill_between(stock['TRADE_DATE'], stock['LOW_PRICE'], stock['HIGH_PRICE'], alpha=0.15, color=COLORS['primary'])
        ax.set_title(f'{ticker}', fontsize=13, fontweight='bold')
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        ax.tick_params(axis='x', rotation=45, labelsize=8)
        if i == 0:
            ax.legend(fontsize=7, loc='upper left')

    plt.tight_layout(rect=[0, 0, 1, 0.94])
    plt.savefig(os.path.join(IMG_DIR, '01_stock_price_trends.png'), dpi=150, bbox_inches='tight')
    plt.close()
    print("  -> 01_stock_price_trends.png")


# ============================================================
# CHART 2: Daily Returns Heatmap
# ============================================================
def chart_daily_returns_heatmap(data):
    """Heatmap of daily returns across all stocks."""
    print("Generating: Daily Returns Heatmap...")
    df = data['portfolio']
    pivot = df.pivot_table(index='TRADE_DATE', columns='TICKER', values='DAILY_RETURN_PCT')
    pivot = pivot.dropna()

    fig, ax = plt.subplots(figsize=(14, 8))
    sns.heatmap(
        pivot.T,
        cmap='RdYlGn',
        center=0,
        annot=True,
        fmt='.1f',
        linewidths=0.5,
        ax=ax,
        cbar_kws={'label': 'Daily Return (%)'},
        annot_kws={'size': 7}
    )
    ax.set_title('Daily Returns Heatmap (%)', fontsize=16, fontweight='bold', pad=20)
    ax.set_xlabel('Trading Date', fontsize=12)
    ax.set_ylabel('Ticker', fontsize=12)
    ax.tick_params(axis='x', rotation=45, labelsize=8)

    plt.tight_layout()
    plt.savefig(os.path.join(IMG_DIR, '02_daily_returns_heatmap.png'), dpi=150, bbox_inches='tight')
    plt.close()
    print("  -> 02_daily_returns_heatmap.png")


# ============================================================
# CHART 3: Sector Performance Analysis
# ============================================================
def chart_sector_performance(data):
    """Sector performance bar chart and trend."""
    print("Generating: Sector Performance...")
    df = data['sector']

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 7))

    # Average return by sector
    sector_avg = df.groupby('SECTOR')['AVG_DAILY_RETURN'].mean().sort_values()
    colors = [COLORS['success'] if x >= 0 else COLORS['danger'] for x in sector_avg.values]
    bars = ax1.barh(sector_avg.index, sector_avg.values, color=colors, edgecolor='white', linewidth=0.5)
    ax1.set_title('Avg Daily Return by Sector', fontsize=14, fontweight='bold')
    ax1.set_xlabel('Average Daily Return (%)')
    ax1.axvline(x=0, color=COLORS['text'], linestyle='-', linewidth=0.5)
    for bar, val in zip(bars, sector_avg.values):
        ax1.text(val + 0.01 if val >= 0 else val - 0.01, bar.get_y() + bar.get_height()/2,
                f'{val:.2f}%', ha='left' if val >= 0 else 'right', va='center', fontsize=9, color=COLORS['text'])

    # Sector volume comparison
    sector_vol = df.groupby('SECTOR')['SECTOR_VOLUME'].sum().sort_values(ascending=True)
    ax2.barh(sector_vol.index, sector_vol.values / 1e6, color=COLORS['teal'], edgecolor='white', linewidth=0.5)
    ax2.set_title('Total Trading Volume by Sector', fontsize=14, fontweight='bold')
    ax2.set_xlabel('Volume (Millions)')

    plt.tight_layout()
    plt.savefig(os.path.join(IMG_DIR, '03_sector_performance.png'), dpi=150, bbox_inches='tight')
    plt.close()
    print("  -> 03_sector_performance.png")


# ============================================================
# CHART 4: Cryptocurrency Dashboard
# ============================================================
def chart_crypto_dashboard(data):
    """Crypto price trends and volatility."""
    print("Generating: Crypto Dashboard...")
    df = data['crypto']
    tickers = df['TICKER'].unique()

    fig, axes = plt.subplots(2, len(tickers), figsize=(20, 10))
    fig.suptitle('Cryptocurrency Market Dashboard', fontsize=18, fontweight='bold')

    crypto_colors = ['#f7931a', '#627eea', '#00d395', '#0033ad', '#23292f']

    for i, ticker in enumerate(tickers):
        coin = df[df['TICKER'] == ticker].sort_values('TRADE_DATE')
        color = crypto_colors[i % len(crypto_colors)]

        # Price chart
        axes[0][i].plot(coin['TRADE_DATE'], coin['CLOSE_PRICE'], color=color, linewidth=2)
        axes[0][i].fill_between(coin['TRADE_DATE'], coin['CLOSE_PRICE'], alpha=0.2, color=color)
        axes[0][i].set_title(ticker.replace('-USD', ''), fontsize=13, fontweight='bold')
        axes[0][i].xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        axes[0][i].tick_params(axis='x', rotation=45, labelsize=7)

        # Volume chart
        axes[1][i].bar(coin['TRADE_DATE'], coin['VOLUME'] / 1e9, color=color, alpha=0.7, width=0.8)
        axes[1][i].set_ylabel('Vol (B)' if i == 0 else '')
        axes[1][i].xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        axes[1][i].tick_params(axis='x', rotation=45, labelsize=7)

    axes[0][0].set_ylabel('Price (USD)', fontsize=11)
    axes[1][0].set_ylabel('Volume (Billions)', fontsize=11)

    plt.tight_layout(rect=[0, 0, 1, 0.94])
    plt.savefig(os.path.join(IMG_DIR, '04_crypto_dashboard.png'), dpi=150, bbox_inches='tight')
    plt.close()
    print("  -> 04_crypto_dashboard.png")


# ============================================================
# CHART 5: Forex Exchange Rates
# ============================================================
def chart_forex_rates(data):
    """Forex rate trends and daily changes."""
    print("Generating: Forex Rates...")
    df = data['forex']
    pairs = df['PAIR'].unique()

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 10))
    fig.suptitle('Foreign Exchange Rates Overview', fontsize=18, fontweight='bold')

    forex_colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728']
    for i, pair in enumerate(pairs):
        fx = df[df['PAIR'] == pair].sort_values('TRADE_DATE')
        label = pair.replace('=X', '')
        ax1.plot(fx['TRADE_DATE'], fx['CLOSE_RATE'], linewidth=2, label=label, color=forex_colors[i])

    ax1.set_title('Exchange Rate Trends', fontsize=14, fontweight='bold')
    ax1.set_ylabel('Exchange Rate')
    ax1.legend(fontsize=10)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))

    # Daily change comparison
    latest = df.sort_values('TRADE_DATE').groupby('PAIR').last()
    colors = [COLORS['success'] if x >= 0 else COLORS['danger'] for x in latest['DAILY_CHANGE_PCT'].values]
    bars = ax2.bar(latest.index.str.replace('=X', ''), latest['DAILY_CHANGE_PCT'], color=colors, edgecolor='white')
    ax2.set_title('Latest Daily Change (%)', fontsize=14, fontweight='bold')
    ax2.set_ylabel('Daily Change (%)')
    ax2.axhline(y=0, color=COLORS['text'], linestyle='-', linewidth=0.5)

    plt.tight_layout(rect=[0, 0, 1, 0.94])
    plt.savefig(os.path.join(IMG_DIR, '05_forex_rates.png'), dpi=150, bbox_inches='tight')
    plt.close()
    print("  -> 05_forex_rates.png")


# ============================================================
# CHART 6: News Sentiment Analysis
# ============================================================
def chart_news_sentiment(data):
    """News sentiment distribution and analysis."""
    print("Generating: News Sentiment Analysis...")
    df = data['news']

    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('Financial News Sentiment Analysis (NLP)', fontsize=18, fontweight='bold')

    # Sentiment distribution
    sentiment_colors = {'positive': COLORS['success'], 'negative': COLORS['danger'], 'neutral': COLORS['secondary']}
    counts = df['SENTIMENT_LABEL'].value_counts()
    axes[0][0].pie(counts.values, labels=counts.index, autopct='%1.1f%%',
                   colors=[sentiment_colors.get(l, COLORS['primary']) for l in counts.index],
                   textprops={'color': COLORS['text'], 'fontsize': 12},
                   wedgeprops={'edgecolor': COLORS['bg'], 'linewidth': 2})
    axes[0][0].set_title('Sentiment Distribution', fontsize=14, fontweight='bold')

    # Sentiment score by ticker
    ticker_sentiment = df.groupby('TICKER')['SENTIMENT_SCORE'].mean().sort_values()
    colors = [COLORS['success'] if x >= 0 else COLORS['danger'] for x in ticker_sentiment.values]
    axes[0][1].barh(ticker_sentiment.index, ticker_sentiment.values, color=colors, edgecolor='white', linewidth=0.5)
    axes[0][1].set_title('Avg Sentiment Score by Ticker', fontsize=14, fontweight='bold')
    axes[0][1].axvline(x=0, color=COLORS['text'], linestyle='-', linewidth=0.5)
    axes[0][1].set_xlabel('Sentiment Score (-1 to +1)')

    # Sentiment score distribution histogram
    axes[1][0].hist(df['SENTIMENT_SCORE'], bins=20, color=COLORS['primary'], edgecolor='white', alpha=0.8)
    axes[1][0].axvline(x=0, color=COLORS['danger'], linestyle='--', linewidth=1.5, label='Neutral')
    axes[1][0].axvline(x=df['SENTIMENT_SCORE'].mean(), color=COLORS['success'], linestyle='--', linewidth=1.5,
                       label=f'Mean: {df["SENTIMENT_SCORE"].mean():.3f}')
    axes[1][0].set_title('Sentiment Score Distribution', fontsize=14, fontweight='bold')
    axes[1][0].set_xlabel('Sentiment Score')
    axes[1][0].set_ylabel('Count')
    axes[1][0].legend(fontsize=10)

    # Article count by ticker
    article_count = df.groupby('TICKER').size().sort_values(ascending=True)
    axes[1][1].barh(article_count.index, article_count.values, color=COLORS['teal'], edgecolor='white', linewidth=0.5)
    axes[1][1].set_title('News Article Count by Ticker', fontsize=14, fontweight='bold')
    axes[1][1].set_xlabel('Number of Articles')

    plt.tight_layout(rect=[0, 0, 1, 0.94])
    plt.savefig(os.path.join(IMG_DIR, '06_news_sentiment.png'), dpi=150, bbox_inches='tight')
    plt.close()
    print("  -> 06_news_sentiment.png")


# ============================================================
# CHART 7: Risk vs Return Scatter Plot
# ============================================================
def chart_risk_return(data):
    """Risk-return scatter plot for portfolio analysis."""
    print("Generating: Risk vs Return...")
    df = data['portfolio']

    stats = df.groupby('TICKER').agg(
        avg_return=('DAILY_RETURN_PCT', 'mean'),
        volatility=('VOLATILITY_30D', 'mean'),
        avg_price=('CLOSE_PRICE', 'mean'),
        company=('COMPANY_NAME', 'first'),
        sector=('SECTOR', 'first')
    ).reset_index()
    stats['sector'] = stats['sector'].fillna('Other')

    fig, ax = plt.subplots(figsize=(14, 9))

    sectors = stats['sector'].unique()
    sector_colors = dict(zip(sectors, sns.color_palette("husl", len(sectors))))

    for _, row in stats.iterrows():
        color = sector_colors.get(row['sector'], COLORS['primary'])
        size = max(row['avg_price'] / 2, 50)
        ax.scatter(row['volatility'], row['avg_return'], s=size, color=color, alpha=0.8, edgecolors='white', linewidth=1.5)
        ax.annotate(row['TICKER'], (row['volatility'], row['avg_return']),
                   textcoords="offset points", xytext=(8, 8), fontsize=11, fontweight='bold', color=COLORS['text'])

    ax.axhline(y=0, color=COLORS['text'], linestyle='--', linewidth=0.5, alpha=0.5)
    ax.set_title('Risk vs Return Analysis (Bubble Size = Avg Price)', fontsize=16, fontweight='bold')
    ax.set_xlabel('30-Day Volatility (Risk)', fontsize=13)
    ax.set_ylabel('Average Daily Return (%)', fontsize=13)

    # Legend for sectors
    for sector, color in sector_colors.items():
        if sector:
            ax.scatter([], [], c=[color], s=80, label=sector)
    ax.legend(loc='upper left', fontsize=9, title='Sector', title_fontsize=10)

    plt.tight_layout()
    plt.savefig(os.path.join(IMG_DIR, '07_risk_return.png'), dpi=150, bbox_inches='tight')
    plt.close()
    print("  -> 07_risk_return.png")


# ============================================================
# CHART 8: Volume Analysis
# ============================================================
def chart_volume_analysis(data):
    """Trading volume analysis."""
    print("Generating: Volume Analysis...")
    df = data['portfolio']

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 10))
    fig.suptitle('Trading Volume Analysis', fontsize=18, fontweight='bold')

    # Stacked volume over time
    pivot_vol = df.pivot_table(index='TRADE_DATE', columns='TICKER', values='VOLUME')
    pivot_vol = pivot_vol.fillna(0) / 1e6
    pivot_vol.plot.area(ax=ax1, alpha=0.7, linewidth=0.5)
    ax1.set_title('Daily Trading Volume by Stock (Millions)', fontsize=14, fontweight='bold')
    ax1.set_ylabel('Volume (Millions)')
    ax1.legend(fontsize=8, ncol=5, loc='upper left')
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))

    # Average volume bar chart
    avg_vol = df.groupby('TICKER')['VOLUME'].mean().sort_values(ascending=True) / 1e6
    ax2.barh(avg_vol.index, avg_vol.values, color=COLORS['primary'], edgecolor='white', linewidth=0.5)
    ax2.set_title('Average Daily Volume by Stock (Millions)', fontsize=14, fontweight='bold')
    ax2.set_xlabel('Avg Volume (Millions)')
    for i, (ticker, vol) in enumerate(avg_vol.items()):
        ax2.text(vol + 0.5, i, f'{vol:.1f}M', va='center', fontsize=9, color=COLORS['text'])

    plt.tight_layout(rect=[0, 0, 1, 0.94])
    plt.savefig(os.path.join(IMG_DIR, '08_volume_analysis.png'), dpi=150, bbox_inches='tight')
    plt.close()
    print("  -> 08_volume_analysis.png")


# ============================================================
# CHART 9: Cumulative Returns Comparison
# ============================================================
def chart_cumulative_returns(data):
    """Cumulative returns over time."""
    print("Generating: Cumulative Returns...")
    df = data['portfolio']

    fig, ax = plt.subplots(figsize=(16, 9))

    for ticker in df['TICKER'].unique():
        stock = df[df['TICKER'] == ticker].sort_values('TRADE_DATE')
        ax.plot(stock['TRADE_DATE'], stock['CUMULATIVE_RETURN_PCT'], linewidth=2, label=ticker)

    ax.axhline(y=0, color=COLORS['text'], linestyle='--', linewidth=0.5, alpha=0.5)
    ax.set_title('Cumulative Returns Comparison', fontsize=16, fontweight='bold')
    ax.set_xlabel('Date', fontsize=13)
    ax.set_ylabel('Cumulative Return (%)', fontsize=13)
    ax.legend(fontsize=10, ncol=5, loc='upper left')
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))

    plt.tight_layout()
    plt.savefig(os.path.join(IMG_DIR, '09_cumulative_returns.png'), dpi=150, bbox_inches='tight')
    plt.close()
    print("  -> 09_cumulative_returns.png")


# ============================================================
# CHART 10: Executive KPI Dashboard
# ============================================================
def chart_executive_dashboard(data):
    """Executive summary KPI dashboard."""
    print("Generating: Executive Dashboard...")
    df = data['summary']
    portfolio = data['portfolio']

    fig = plt.figure(figsize=(20, 12))
    fig.suptitle('Finance Data Analytics - Executive Dashboard', fontsize=22, fontweight='bold', y=0.98)

    # Latest metrics
    latest = df.sort_values('TRADE_DATE').iloc[-1]

    # KPI Cards (top row)
    kpis = [
        ('Total Stocks', f'{int(latest["TOTAL_STOCKS_TRACKED"])}', COLORS['primary']),
        ('Market Avg Return', f'{latest["MARKET_AVG_RETURN"]:.2f}%', COLORS['success'] if latest['MARKET_AVG_RETURN'] >= 0 else COLORS['danger']),
        ('Top Gainer', f'{latest["TOP_GAINER_TICKER"]}\n{latest["TOP_GAINER_RETURN"]:.2f}%', COLORS['success']),
        ('Top Loser', f'{latest["TOP_LOSER_TICKER"]}\n{latest["TOP_LOSER_RETURN"]:.2f}%', COLORS['danger']),
        ('Avg Volatility', f'{latest["AVG_MARKET_VOLATILITY"]:.2f}', COLORS['secondary']),
    ]

    for i, (title, value, color) in enumerate(kpis):
        ax = fig.add_axes([0.02 + i * 0.196, 0.78, 0.18, 0.15])
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.add_patch(plt.Rectangle((0.05, 0.05), 0.9, 0.9, facecolor=color, alpha=0.15, edgecolor=color, linewidth=2, transform=ax.transAxes))
        ax.text(0.5, 0.65, value, ha='center', va='center', fontsize=18, fontweight='bold', color=color)
        ax.text(0.5, 0.25, title, ha='center', va='center', fontsize=11, color=COLORS['text'])
        ax.axis('off')

    # Market trend (bottom left)
    ax1 = fig.add_axes([0.06, 0.08, 0.42, 0.6])
    ax1.plot(df['TRADE_DATE'], df['MARKET_AVG_RETURN'], color=COLORS['primary'], linewidth=2, marker='o', markersize=4)
    ax1.fill_between(df['TRADE_DATE'], df['MARKET_AVG_RETURN'], alpha=0.2, color=COLORS['primary'])
    ax1.axhline(y=0, color=COLORS['danger'], linestyle='--', linewidth=0.5)
    ax1.set_title('Market Average Return Trend', fontsize=14, fontweight='bold')
    ax1.set_ylabel('Avg Return (%)')
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
    ax1.tick_params(axis='x', rotation=45)

    # Volume trend (bottom right)
    ax2 = fig.add_axes([0.56, 0.08, 0.42, 0.6])
    ax2.bar(df['TRADE_DATE'], df['TOTAL_MARKET_VOLUME'] / 1e9, color=COLORS['teal'], alpha=0.7, width=0.8)
    ax2.set_title('Total Market Volume Trend', fontsize=14, fontweight='bold')
    ax2.set_ylabel('Volume (Billions)')
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
    ax2.tick_params(axis='x', rotation=45)

    plt.savefig(os.path.join(IMG_DIR, '10_executive_dashboard.png'), dpi=150, bbox_inches='tight')
    plt.close()
    print("  -> 10_executive_dashboard.png")


# ============================================================
# CHART 11: Correlation Matrix
# ============================================================
def chart_correlation_matrix(data):
    """Stock return correlation matrix."""
    print("Generating: Correlation Matrix...")
    df = data['portfolio']
    pivot = df.pivot_table(index='TRADE_DATE', columns='TICKER', values='DAILY_RETURN_PCT')
    corr = pivot.corr()

    fig, ax = plt.subplots(figsize=(12, 10))
    mask = [[False]*len(corr) for _ in range(len(corr))]
    for i in range(len(corr)):
        for j in range(i):
            mask[i][j] = True

    sns.heatmap(corr, annot=True, fmt='.2f', cmap='RdYlGn', center=0,
                square=True, linewidths=1, ax=ax,
                cbar_kws={'label': 'Correlation'},
                annot_kws={'size': 10})
    ax.set_title('Stock Return Correlation Matrix', fontsize=16, fontweight='bold', pad=20)

    plt.tight_layout()
    plt.savefig(os.path.join(IMG_DIR, '11_correlation_matrix.png'), dpi=150, bbox_inches='tight')
    plt.close()
    print("  -> 11_correlation_matrix.png")


# ============================================================
# MAIN
# ============================================================
def main():
    print("=" * 60)
    print("FINANCE DATA ANALYTICS - GENERATING ALL CHARTS")
    print("=" * 60)

    data = load_data()

    print("\nGenerating visualizations...")
    chart_stock_price_trends(data)
    chart_daily_returns_heatmap(data)
    chart_sector_performance(data)
    chart_crypto_dashboard(data)
    chart_forex_rates(data)
    chart_news_sentiment(data)
    chart_risk_return(data)
    chart_volume_analysis(data)
    chart_cumulative_returns(data)
    chart_executive_dashboard(data)
    chart_correlation_matrix(data)

    print("\n" + "=" * 60)
    print(f"ALL 11 CHARTS GENERATED in {IMG_DIR}/")
    print("=" * 60)


if __name__ == "__main__":
    main()

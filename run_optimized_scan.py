#!/usr/bin/env python3
"""Optimized full market scanner with parallel processing.

This version uses parallel workers to achieve 10-25 TPS safely while
avoiding rate limits through:
- Thread pool with 5 workers
- Per-worker rate limiting (0.2s = 5 TPS each)
- Adaptive backoff on errors
- Session pooling

Expected runtime: 15-30 minutes for 3,800+ stocks

Usage:
    python run_optimized_scan.py
    python run_optimized_scan.py --workers 10  # Faster but riskier
    python run_optimized_scan.py --conservative  # Slower but safer (3 workers)
"""

import argparse
import json
import logging
import sys
import os
import time
import io
from datetime import datetime
from pathlib import Path
from collections import Counter

# Force UTF-8 encoding for stdout to prevent Unicode crashes on Windows consoles
encoding = (sys.stdout.encoding or '').lower()
if encoding != 'utf-8' and hasattr(sys.stdout, 'buffer'):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from src.config.settings import SettingsValidationError, load_settings
from src.data.universe_fetcher import StockUniverseFetcher
from src.screening.optimized_batch_processor import OptimizedBatchProcessor
from src.screening.benchmark import (
    analyze_benchmark_trend,
    calculate_market_breadth,
    format_benchmark_summary,
    should_generate_signals
)
from src.screening.signal_engine import score_buy_signal, score_sell_signal
from src.data.enhanced_fundamentals import EnhancedFundamentalsFetcher
from src.reporting.newsletter_generator import NewsletterGenerator
from src.reporting.portfolio_manager import PortfolioManager
from src.reporting.performance_tracker import PerformanceTracker
from src.notifications.email_notifier import EmailNotifier
from src.database.db_manager import DBManager
from src.utils.logging_config import configure_logging


logger = logging.getLogger(__name__)


def _write_github_output(key: str, value: str) -> None:
    """Write step output when running under GitHub Actions."""
    github_output_path = os.getenv('GITHUB_OUTPUT')
    if not github_output_path:
        return
    try:
        with open(github_output_path, 'a', encoding='utf-8') as output_file:
            output_file.write(f"{key}={value}\n")
    except Exception as output_err:
        logger.warning(f"Unable to write {key} to GITHUB_OUTPUT: {output_err}")


def _emit_email_delivery_summary(summary: dict) -> None:
    """Log and export a structured email delivery summary."""
    summary_json = json.dumps(summary, sort_keys=True)
    logger.info(f"EMAIL_DELIVERY_SUMMARY {summary_json}")

    _write_github_output('email_summary_json', summary_json)
    _write_github_output('email_recipients_targeted', str(summary['recipients_targeted']))
    _write_github_output('email_attempts', str(summary['attempts']))
    _write_github_output('email_successes', str(summary['successes']))
    _write_github_output('email_failures', str(summary['failures']))
    _write_github_output('email_top_failure_reason', summary['top_failure_reason'])


def _write_newsletter_fallback_artifacts(root_cause: str) -> str:
    """Write degraded-mode newsletter artifacts so downstream freshness checks can proceed."""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    date_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    root_cause_clean = (root_cause or 'unknown_error').strip() or 'unknown_error'

    lines = [
        '# ⚠️ DEGRADED MODE — DAILY NEWSLETTER FALLBACK',
        '',
        f'**Generated:** {date_str}',
        f'**Root Cause:** `{root_cause_clean}`',
        '',
        'The full newsletter pipeline failed during this run. This fallback artifact is intentionally minimal and non-ideal.',
        'Downstream consumers should treat this as degraded content while freshness remains intact.',
        '',
        '## Operational Status',
        '- Newsletter generation failed and fallback markdown was emitted.',
        '- Investigate logs for stack trace and upstream dependency health.',
        '',
        '## Disclaimer',
        'This content is for operational continuity only and is not investment advice.',
        '',
    ]
    fallback_md = '\n'.join(lines)

    newsletters_dir = Path('./data/newsletters')
    newsletters_dir.mkdir(parents=True, exist_ok=True)
    fallback_path = newsletters_dir / f'daily_newsletter_{timestamp}.md'
    fallback_path.write_text(fallback_md, encoding='utf-8')

    stable_path = Path('./data/daily_newsletter.md')
    stable_path.parent.mkdir(parents=True, exist_ok=True)
    stable_path.write_text(fallback_md, encoding='utf-8')

    return str(fallback_path)


def save_report(results, buy_signals, sell_signals, benchmark_analysis, breadth, output_dir="./data/daily_scans"):
    """Save comprehensive report."""
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    date_str = datetime.now().strftime('%Y-%m-%d')

    output = []
    output.append("="*80)
    output.append("OPTIMIZED FULL MARKET SCAN - NSE INDIA STOCKS")
    output.append(f"Scan Date: {date_str}")
    output.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    output.append("="*80)
    output.append("")

    # Stats
    output.append("SCANNING STATISTICS")
    output.append("-"*80)
    output.append(f"Total Universe: {results['total_processed']:,} stocks")
    output.append(f"Analyzed: {results['total_analyzed']:,} stocks")
    output.append(f"Processing Time: {results['processing_time_seconds']/60:.1f} minutes")
    output.append(f"Actual TPS: {results['actual_tps']:.2f}")

    error_rate = results['error_rate'] * 100
    if error_rate < 1:
        error_emoji = "🟢"
    elif error_rate < 5:
        error_emoji = "🟡"
    else:
        error_emoji = "🔴"
    output.append(f"{error_emoji} Error Rate: {error_rate:.2f}%")

    # Buy/Sell signal counts with emoji
    if len(buy_signals) > 0:
        output.append(f"🟢 Buy Signals: {len(buy_signals)}")
    else:
        output.append(f"Buy Signals: {len(buy_signals)}")

    if len(sell_signals) > 0:
        output.append(f"🔴 Sell Signals: {len(sell_signals)}")
    else:
        output.append(f"Sell Signals: {len(sell_signals)}")
    output.append("")

    # Benchmark
    output.append(format_benchmark_summary(benchmark_analysis, breadth))
    output.append("")

    # Buy signals
    output.append("="*80)
    output.append(f"🟢 TOP BUY SIGNALS (Score >= 70) - {len(buy_signals)} Total")
    output.append("="*80)
    output.append("")

    if buy_signals:
        for i, signal in enumerate(buy_signals[:50], 1):
            score = signal['score']
            # Score-based emoji (green/yellow with star for exceptional)
            if score >= 90:
                score_emoji = "⭐"  # Exceptional - star
            elif score >= 80:
                score_emoji = "🟢"  # Very good - green
            elif score >= 70:
                score_emoji = "🟢"  # Good - green
            else:
                score_emoji = "🟡"  # Borderline - yellow

            output.append(f"\n{'#'*80}")
            output.append(f"{score_emoji} BUY #{i}: {signal['ticker']} | Score: {signal['score']}/110")
            output.append(f"{'#'*80}")
            output.append(f"Phase: {signal['phase']}")

            # Entry quality with emoji
            entry_quality = signal.get('entry_quality', 'Unknown')
            if entry_quality == 'Good':
                output.append(f"🟢 Entry Quality: {entry_quality}")
            elif entry_quality == 'Extended':
                output.append(f"🟡 Entry Quality: {entry_quality}")
            else:
                output.append(f"🔴 Entry Quality: {entry_quality}")

            # CRITICAL: Stop loss and R/R ratio
            if signal.get('stop_loss'):
                output.append(f"Stop Loss: {signal['stop_loss']:.2f}")
                details = signal.get('details', {})
                risk_amt = details.get('risk_amount', 0)
                reward_amt = details.get('reward_amount', 0)
                rr_ratio = signal.get('risk_reward_ratio', 0)
                # R/R ratio emoji
                if rr_ratio >= 3:
                    rr_emoji = "🟢"  # Excellent R/R
                elif rr_ratio >= 2:
                    rr_emoji = "🟢"  # Good R/R
                else:
                    rr_emoji = "🟡"  # Poor R/R
                output.append(f"{rr_emoji} Risk/Reward: {rr_ratio:.1f}:1 (Risk {risk_amt:.2f}, Reward {reward_amt:.2f})")

            if signal.get('breakout_price'):
                output.append(f"Breakout: {signal['breakout_price']:.2f}")

            details = signal.get('details', {})
            if 'rs_slope' in details:
                rs_slope = details['rs_slope']
                # RS emoji (green = good, yellow = ok, red = bad)
                if rs_slope > 0.5:
                    rs_emoji = "🟢"  # Strong RS
                elif rs_slope > 0:
                    rs_emoji = "🟡"  # Positive RS
                else:
                    rs_emoji = "🔴"  # Weak RS
                output.append(f"{rs_emoji} RS: {rs_slope:.3f}")
            if 'volume_ratio' in details:
                vol_ratio = details['volume_ratio']
                # Volume emoji
                if vol_ratio > 1.5:
                    vol_emoji = "🟢"  # High volume
                elif vol_ratio > 1.0:
                    vol_emoji = "🟡"  # Above average
                else:
                    vol_emoji = "🔴"  # Low volume
                output.append(f"{vol_emoji} Volume: {vol_ratio:.1f}x")

            # VCP pattern details if detected
            vcp_data = details.get('vcp_data')
            if vcp_data:
                vcp_quality = vcp_data.get('quality', 0)
                contractions = vcp_data.get('contractions', 0)
                pattern = vcp_data.get('pattern', 'N/A')

                if vcp_quality >= 80:
                    vcp_emoji = "⭐"  # Exceptional VCP
                elif vcp_quality >= 60:
                    vcp_emoji = "🟢"  # Good VCP
                elif vcp_quality >= 50:
                    vcp_emoji = "🟡"  # Marginal VCP
                else:
                    vcp_emoji = "🟡"  # Partial pattern

                if vcp_quality >= 50:
                    output.append(f"{vcp_emoji} VCP: {pattern} (quality: {vcp_quality:.0f}/100)")

            output.append("\nKey Reasons:")
            for reason in signal['reasons'][:7]:  # Show 7 instead of 5
                output.append(f"  • {reason}")

            if signal.get('fundamental_snapshot'):
                output.append(signal['fundamental_snapshot'])

        if len(buy_signals) > 50:
            output.append(f"\n{'='*80}")
            output.append(f"ADDITIONAL BUYS ({len(buy_signals)-50} more)")
            output.append(f"{'='*80}\n")
            remaining = [s['ticker'] for s in buy_signals[50:]]
            for i in range(0, len(remaining), 10):
                output.append(", ".join(remaining[i:i+10]))
    else:
        output.append("✗ NO BUY SIGNALS TODAY")

    # Sell signals
    output.append(f"\n\n{'='*80}")
    output.append(f"🔴 TOP SELL SIGNALS (Score >= 60) - {len(sell_signals)} Total")
    output.append(f"{'='*80}")
    output.append("")

    if sell_signals:
        for i, signal in enumerate(sell_signals[:30], 1):
            score = signal['score']
            severity = signal['severity']

            # Severity emoji (red/yellow with alarm for critical)
            if severity == 'critical':
                severity_emoji = "🚨"  # Critical - alarm
            elif severity == 'high':
                severity_emoji = "🔴"  # High - red
            else:
                severity_emoji = "🟡"  # Moderate - yellow

            # Score emoji (higher score = more urgent to sell)
            if score >= 80:
                score_emoji = "🚨"  # Very urgent - alarm
            elif score >= 70:
                score_emoji = "🔴"  # Urgent - red
            else:
                score_emoji = "🟡"  # Warning - yellow

            output.append(f"\n{'#'*80}")
            output.append(f"{score_emoji} SELL #{i}: {signal['ticker']} | Score: {signal['score']}/110")
            output.append(f"{'#'*80}")
            output.append(f"Phase: {signal['phase']} | {severity_emoji} Severity: {severity.upper()}")
            if signal.get('breakdown_level'):
                output.append(f"Breakdown: {signal['breakdown_level']:.2f}")
            details = signal.get('details', {})
            if 'rs_slope' in details:
                rs_slope = details['rs_slope']
                # RS emoji for sell signals (negative is expected)
                if rs_slope < -0.5:
                    rs_emoji = "🔴"  # Very weak RS
                elif rs_slope < 0:
                    rs_emoji = "🟡"  # Weak RS
                else:
                    rs_emoji = "🟢"  # Still positive RS (unusual for sell)
                output.append(f"{rs_emoji} RS: {rs_slope:.3f}")
            output.append("\nSell Reasons:")
            for reason in signal['reasons'][:5]:
                output.append(f"  • {reason}")

            if signal.get('fundamental_snapshot'):
                output.append(signal['fundamental_snapshot'])

        if len(sell_signals) > 30:
            output.append(f"\n{'='*80}")
            output.append(f"ADDITIONAL SELLS ({len(sell_signals)-30} more)")
            output.append(f"{'='*80}\n")
            remaining = [s['ticker'] for s in sell_signals[30:]]
            for i in range(0, len(remaining), 10):
                output.append(", ".join(remaining[i:i+10]))
    else:
        output.append("✗ NO SELL SIGNALS TODAY")

    output.append(f"\n\n{'='*80}")
    output.append("END OF SCAN")
    output.append(f"{'='*80}\n")

    report_text = "\n".join(output)

    # Save
    filepath = Path(output_dir) / f"optimized_scan_{timestamp}.txt"
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(report_text)

    latest_path = Path(output_dir) / "latest_optimized_scan.txt"
    with open(latest_path, 'w', encoding='utf-8') as f:
        f.write(report_text)

    logger.info(f"Report saved: {filepath}")
    print(report_text)

    return filepath


def main():
    parser = argparse.ArgumentParser(description='Optimized Full Market Scanner')
    parser.add_argument('--workers', type=int, default=5, help='Parallel workers (default: 5)')
    parser.add_argument('--delay', type=float, default=0.2, help='Delay per worker (default: 0.2s)')
    parser.add_argument('--conservative', action='store_true', help='Ultra-conservative mode (2 workers, 1.0s delay)')
    parser.add_argument('--aggressive', action='store_true', help='Faster mode (5 workers, 0.3s delay) - MAY HIT RATE LIMITS!')
    parser.add_argument('--resume', action='store_true', help='Resume from progress')
    parser.add_argument('--clear-progress', action='store_true', help='Clear progress')
    parser.add_argument('--limit', type=int, help='Limit scan to first N stocks (e.g. 50)')
    parser.add_argument('--tickers', type=str, help='Comma-separated list of specific NSE tickers (e.g. RELIANCE.NS,TCS.NS,INFY.NS)')
    parser.add_argument('--test-mode', action='store_true', help='Test with 100 stocks')
    parser.add_argument('--min-price', type=float, default=5.0, help='Min price')
    parser.add_argument('--min-volume', type=int, default=100000, help='Min volume')
    parser.add_argument('--etfs', action='store_true', help='Scan NSE ETFs instead of stocks')
    parser.add_argument('--git-storage', action='store_true', help='Use Git-based storage for fundamentals (recommended)')
    parser.add_argument('--send-email', action='store_true', help='Force-enable newsletter email delivery')
    parser.add_argument('--no-email', action='store_true', help='Disable newsletter email delivery')
    parser.add_argument('--strict-email', action='store_true',
                        help='Fail run when email delivery has zero successful sends')
    parser.add_argument('--diagnostics', action='store_true', help='Run diagnostic check for API keys and basic access')
    parser.add_argument('--universe-source', type=str, default='exchange', choices=['auto','exchange'],
                        help='Universe source preference (default: exchange)')
    parser.add_argument('--prefetch-storage', action='store_true', help='Warm git storage fundamentals before scan')
    parser.add_argument('--no-prefetch-storage', action='store_true', help='Skip warm-up storage pass before scan')
    parser.add_argument('--log-level', type=str, default=os.getenv('LOG_LEVEL', 'INFO'),
                        help='Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)')
    parser.add_argument('--json-logs', action='store_true', help='Emit structured JSON logs')

    args = parser.parse_args()
    configure_logging(level=args.log_level, json_logs=args.json_logs)

    try:
        app_settings = load_settings("config.yaml")
    except SettingsValidationError as exc:
        logger.error("Configuration validation failed: %s", exc)
        sys.exit(2)

    # By default, prefetch storage when git-storage mode is used
    args.prefetch_storage = (args.prefetch_storage or args.git_storage) and not args.no_prefetch_storage

    # Email defaults: enabled when configured unless explicitly disabled
    send_email_explicitly_requested = args.send_email
    send_email_default = os.getenv('SEND_NEWSLETTER_EMAIL', '1').strip().lower() not in {'0', 'false', 'no'}
    args.send_email = (args.send_email or send_email_default) and not args.no_email

    # Presets
    if args.conservative:
        args.workers = 2
        args.delay = 1.0
        logger.info("Ultra-conservative mode: 2 workers, 1.0s delay (~2 TPS)")
    elif args.aggressive:
        args.workers = 5
        args.delay = 0.3
        logger.warning("Aggressive mode: 5 workers, 0.3s delay (~17 TPS) - MAY HIT RATE LIMITS!")

    effective_tps = args.workers / args.delay
    logger.info(f"Configuration: {args.workers} workers × {1/args.delay:.1f} TPS = ~{effective_tps:.1f} TPS effective")

    # Diagnostics mode
    if args.diagnostics:
        logger.info("="*60)
        logger.info("DIAGNOSTIC CHECK")
        logger.info("="*60)
        
        # 1. Check Email
        n = EmailNotifier()
        if n.enabled:
            logger.info(f"✓ Email: Configured (Sender: {n.sender_email})")
        else:
            logger.warning("• Email: Not configured (Optional)")
            
        logger.info("="*60)
        sys.exit(0)

    # Initialize enhanced fundamentals fetcher
    fundamentals_fetcher = EnhancedFundamentalsFetcher()

    try:
        # Fetch universe
        universe_fetcher = StockUniverseFetcher()
        logger.info(f"Fetching {'ETF' if args.etfs else 'stock'} universe...")
        tickers = universe_fetcher.fetch_universe(include_etfs=args.etfs)

        if not tickers:
            logger.error("Failed to fetch universe")
            sys.exit(1)

        logger.info(f"Universe: {len(tickers):,} stocks (source preference: {args.universe_source})")

        if args.tickers:
            tickers = [t.strip().upper() for t in args.tickers.split(',')]
            logger.info(f"CUSTOM TICKER LIST: {len(tickers)} stocks")
        elif args.limit:
            tickers = tickers[:args.limit]
            logger.info(f"LIMITED MODE: {len(tickers)} stocks")
        elif args.test_mode:
            tickers = tickers[:100]
            logger.info(f"TEST MODE: {len(tickers)} stocks")

        # Initialize processor
        processor = OptimizedBatchProcessor(
            settings=app_settings.optimized_batch_processor,
            fetcher_settings=app_settings.fetcher,
            max_workers=args.workers,
            rate_limit_delay=args.delay,
            use_git_storage=args.git_storage,
        )

        if args.git_storage:
            logger.info("Git-based fundamental storage enabled - 74% API call reduction!")

        logger.info("Using standard yfinance fundamentals.")


        if args.clear_progress:
            processor.clear_progress()

        # Optional storage-first warm-up
        if args.prefetch_storage and args.git_storage:
            logger.info("Running storage-first fundamentals warm-up before scan...")
            processor.prefetch_fundamentals_storage(tickers)

        # Process
        results = processor.process_batch_parallel(
            tickers,
            resume=args.resume,
            min_price=args.min_price,
            min_volume=args.min_volume
        )

        if 'error' in results:
            logger.error(results['error'])
            sys.exit(1)

        # Analysis
        logger.info("Generating signals...")
        benchmark_analysis = analyze_benchmark_trend(processor.benchmark_data, processor.benchmark_price)
        breadth = calculate_market_breadth(results['phase_results'])
        signal_rec = should_generate_signals(benchmark_analysis, breadth)

        # Buy signals
        buy_signals = []
        if signal_rec['should_generate_buys']:
            preliminary_buys = []
            for analysis in results['analyses']:
                if analysis['phase_info']['phase'] in [1, 2]:
                    # Quick preliminary score
                    signal = score_buy_signal(
                        ticker=analysis['ticker'],
                        price_data=analysis['price_data'],
                        current_price=analysis['current_price'],
                        phase_info=analysis['phase_info'],
                        rs_series=analysis['rs_series'],
                        fundamentals=analysis.get('quarterly_data'),
                        vcp_data=analysis.get('vcp_data')
                    )
                    if signal['is_buy']:
                        preliminary_buys.append((analysis, signal))

            # Sort and take top 15 candidates for final scoring
            preliminary_buys = sorted(preliminary_buys, key=lambda x: x[1]['score'], reverse=True)[:15]

            logger.info(f"Final-scoring top {len(preliminary_buys)} buy candidates...")
            for analysis, _ in preliminary_buys:
                ticker = analysis['ticker']

                final_signal = score_buy_signal(
                    ticker=ticker,
                    price_data=analysis['price_data'],
                    current_price=analysis['current_price'],
                    phase_info=analysis['phase_info'],
                    rs_series=analysis['rs_series'],
                    fundamentals=analysis.get('quarterly_data'),
                    vcp_data=analysis.get('vcp_data')
                )

                # Attach extras for the report
                final_signal['fundamental_snapshot'] = fundamentals_fetcher.create_snapshot(
                    ticker,
                    quarterly_data=analysis.get('quarterly_data', {})
                )
                final_signal['technical_signals'] = analysis.get('technical_signals', {})

                buy_signals.append(final_signal)

        buy_signals = sorted(buy_signals, key=lambda x: x['score'], reverse=True)

        # Sell signals
        sell_signals = []
        if signal_rec['should_generate_sells']:
            for analysis in results['analyses']:
                if analysis['phase_info']['phase'] in [3, 4]:
                    signal = score_sell_signal(
                        ticker=analysis['ticker'],
                        price_data=analysis['price_data'],
                        current_price=analysis['current_price'],
                        phase_info=analysis['phase_info'],
                        rs_series=analysis['rs_series'],
                        fundamentals=analysis.get('quarterly_data')  # Pass raw quarterly data, not analyzed
                    )
                    if signal['is_sell']:
                        # Add fundamental snapshot
                        signal['fundamental_snapshot'] = fundamentals_fetcher.create_snapshot(
                            analysis['ticker'],
                            quarterly_data=analysis.get('quarterly_data', {})
                        )
                        signal['technical_signals'] = analysis.get('technical_signals', {})
                        sell_signals.append(signal)

        sell_signals = sorted(sell_signals, key=lambda x: x['score'], reverse=True)

        # Report
        save_report(results, buy_signals, sell_signals, benchmark_analysis, breadth)

        # Record Recommendations & Generate Portfolio Reports
        try:
            logger.info("Recording signals and generating portfolio management reports...")
            db = DBManager()
            # Record current signals for historical alpha tracking
            # Combine buys and sells for the database record
            all_signals = buy_signals + sell_signals
            db.record_recommendations(all_signals, benchmark_price=processor.benchmark_price)
            
            # Generate advanced reports (Allocation, Rebalance, Alpha Tracker)
            pm = PortfolioManager()
            pm.generate_reports(buy_signals, sell_signals)
        except Exception as pm_err:
            logger.error(f"Failed to record signals or generate portfolio reports: {pm_err}")

        # ===== PERFORMANCE TRACKER =====
        fund_performance_md = ""
        try:
            logger.info("🏦 Running AlphaIntelligence Capital performance tracker...")
            tracker = PerformanceTracker(strategy='DAILY')
            
            # 1. Process new signals → open/close positions
            tracker.process_signals(buy_signals, sell_signals, benchmark_price=processor.benchmark_price)
            
            # 2. Check stop-losses on all open positions
            stopped_out = tracker.check_stop_losses()
            if stopped_out:
                logger.info(f"🛑 {len(stopped_out)} positions closed via stop-loss")
            
            # 3. Generate newsletter section with fund metrics
            fund_performance_md = tracker.get_newsletter_section()
            logger.info("✅ Performance tracker complete")
        except Exception as tracker_err:
            logger.error(f"Performance tracker error (non-fatal): {tracker_err}")

        # Generate Newsletter
        try:
            logger.info("Generating daily newsletter...")
            newsletter_gen = NewsletterGenerator(ai_settings=app_settings.ai_agent)
            
            # Prepare status dict
            market_status = {
                'benchmark': benchmark_analysis,
                'breadth': breadth
            }
            
            newsletter_path = newsletter_gen.generate_newsletter(
                market_status=market_status,
                top_buys=buy_signals,
                top_sells=sell_signals,
                fund_performance_md=fund_performance_md
            )

            # Maintain stable latest path for CI summaries and downstream consumers.
            latest_newsletter_path = Path('./data/daily_newsletter.md')
            latest_newsletter_path.parent.mkdir(parents=True, exist_ok=True)
            latest_newsletter_path.write_text(Path(newsletter_path).read_text(encoding='utf-8'), encoding='utf-8')
            html_source = Path(newsletter_path).with_suffix('.html')
            if html_source.exists():
                Path('./data/daily_newsletter.html').write_text(html_source.read_text(encoding='utf-8'), encoding='utf-8')

            logger.info(f"Newsletter ready: {newsletter_path}")
            
            # Send Email
            email_summary = {
                'email_enabled': bool(args.send_email),
                'strict_mode': bool(args.strict_email or send_email_explicitly_requested),
                'explicit_send_email': bool(send_email_explicitly_requested),
                'recipients_targeted': 0,
                'attempts': 0,
                'successes': 0,
                'failures': 0,
                'top_failure_reason': 'none',
            }
            email_failure_reasons = []

            if args.send_email:
                try:
                    logger.info("Preparing AlphaIntelligence Capital email delivery...")
                    
                    # Build subscriber list: always include ENV recipient(s), optionally add DB subscribers
                    subscribers = []
                    env_var_source = None
                    env_recipients = []

                    raw_env_recipients = os.getenv('EMAIL_RECIPIENT')
                    if raw_env_recipients:
                        env_var_source = 'EMAIL_RECIPIENT'
                    else:
                        raw_env_recipients = os.getenv('EMAIL_TO')
                        if raw_env_recipients:
                            env_var_source = 'EMAIL_TO'
                            logger.info("EMAIL_RECIPIENT not set; using EMAIL_TO fallback alias for recipients.")

                    if raw_env_recipients:
                        seen_emails = set()
                        for value in raw_env_recipients.split(','):
                            email = value.strip()
                            if not email:
                                continue
                            dedupe_key = email.lower()
                            if dedupe_key in seen_emails:
                                continue
                            seen_emails.add(dedupe_key)
                            env_recipients.append(email)
                        subscribers.extend(env_recipients)

                    # Try to add database subscribers (optional — works without DB)
                    db_added_count = 0
                    try:
                        db = DBManager()
                        db_subs = db.get_active_subscribers()
                        for email in db_subs:
                            if email not in subscribers:
                                subscribers.append(email)
                                db_added_count += 1
                    except Exception as db_err:
                        logger.warning(f"Could not fetch DB subscribers (non-fatal): {db_err}")

                    env_source_label = f"env:{env_var_source}" if env_var_source else "env:none"
                    logger.info(
                        "Recipient sources prepared: %s sanitized recipient(s), db recipient(s) added: %d, merged total: %d",
                        env_source_label,
                        len(env_recipients),
                        db_added_count,
                        len(subscribers),
                    )
                    
                    if not subscribers:
                        logger.warning("No recipients configured. Set EMAIL_RECIPIENT/EMAIL_TO in .env or add subscribers to DB.")
                    else:
                        logger.info(f"Sending newsletter to {len(subscribers)} recipient(s)...")
                        notifier = EmailNotifier()

                        if not notifier.enabled:
                            logger.error("EmailNotifier is DISABLED. Check EMAIL_SENDER and EMAIL_PASSWORD in .env")
                            email_failure_reasons.append('email_notifier_disabled')
                        else:
                            # Use latest_optimized_scan.txt as attachment if it exists
                            latest_report = Path("./data/daily_scans/latest_optimized_scan.txt")
                            report_to_attach = str(latest_report) if latest_report.exists() else None

                            for email in subscribers:
                                email_summary['attempts'] += 1
                                try:
                                    notifier.recipient_email = email
                                    logger.info(f"Sending to {email}...")
                                    if notifier.send_newsletter(
                                        newsletter_path=newsletter_path,
                                        scan_report_path=report_to_attach
                                    ):
                                        email_summary['successes'] += 1
                                        time.sleep(0.5)
                                    else:
                                        logger.error(f"send_newsletter returned False for {email}")
                                        email_failure_reasons.append('send_newsletter_returned_false')
                                except Exception as e:
                                    logger.error(f"Failed to send to {email}: {type(e).__name__}: {e}")
                                    email_failure_reasons.append(type(e).__name__)

                            logger.info(
                                f"✅ Delivery complete: {email_summary['successes']}/{len(subscribers)} successful."
                            )
                            if email_summary['successes'] == 0 and len(subscribers) > 0:
                                logger.error("CRITICAL: All email delivery attempts failed.")
                except Exception as email_err:
                    logger.error(f"Failed to send newsletter email: {email_err}")
                    email_failure_reasons.append(type(email_err).__name__)
            else:
                logger.info("Newsletter email delivery disabled for this run. Use --send-email to force or set SEND_NEWSLETTER_EMAIL=1.")

            email_summary['failures'] = max(email_summary['attempts'] - email_summary['successes'], 0)
            if email_failure_reasons:
                email_summary['top_failure_reason'] = Counter(email_failure_reasons).most_common(1)[0][0]
            _emit_email_delivery_summary(email_summary)

            strict_email = args.strict_email or send_email_explicitly_requested
            if strict_email and email_summary['successes'] == 0:
                logger.error(
                    "Email delivery strict check failed: zero successful sends "
                    f"(attempts={email_summary['attempts']}, recipients={email_summary['recipients_targeted']})."
                )
                sys.exit(2)
            
            # Print preview
            print("\\n" + "="*60) 
            print("DAILY NEWSLETTER PREVIEW")
            print("="*60)
            with open(newsletter_path, 'r', encoding='utf-8') as f:
                # Print first 20 lines
                print("".join(f.readlines()[:20]))
            print("...\\n(See full file for more)")
            
        except Exception as e:
            root_cause = f"{type(e).__name__}: {e}"
            logger.error(f"Failed to generate newsletter: {root_cause}")
            try:
                fallback_newsletter_path = _write_newsletter_fallback_artifacts(root_cause)
                logger.warning(f"Wrote degraded-mode fallback newsletter artifacts: {fallback_newsletter_path}")
            except Exception as fallback_err:
                logger.error(f"Failed to write degraded-mode fallback newsletter artifacts: {fallback_err}")

        logger.info("="*60)

        logger.info("="*60)
        logger.info("SCAN COMPLETE")
        logger.info(f"Time: {results['processing_time_seconds']/60:.1f} minutes")
        logger.info(f"Actual TPS: {results['actual_tps']:.2f}")
        logger.info(f"Buy signals: {len(buy_signals)}")
        logger.info(f"Sell signals: {len(sell_signals)}")
        logger.info("="*60)

    except KeyboardInterrupt:
        logger.info("\nInterrupted - progress saved")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        # Send error alert if email configured
        try:
            import traceback
            notifier = EmailNotifier()
            if notifier.enabled:
                notifier.send_error_alert(
                    error_message=str(e),
                    error_details=traceback.format_exc()
                )
        except Exception as alert_err:
            logger.error(f"Failed to send error alert: {alert_err}")
        sys.exit(1)


if __name__ == '__main__':
    main()

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import pandas as pd
from tabulate import tabulate

from injective_trader.utils.enums import AlertType, AlertSeverity, Side
from injective_trader.domain.account.position import Position
from injective_trader.domain.account.order import Order
from injective_trader.domain.market.market import Market

@dataclass
class Alert:
    """Alert data structure for risk notifications"""
    type: AlertType
    severity: AlertSeverity
    message: str
    timestamp: datetime = field(default_factory=datetime.now)
    data: Dict = field(default_factory=dict)

class PerformanceMetrics:
    """
    Strategy-level performance metrics with risk monitoring
    """

    def __init__(self, logger, start_time: datetime = None):
        # Core dependencies
        self.logger = logger
        self.start_time = start_time or datetime.now()

        # Performance tracking
        self.total_trades: int = 0
        self.winning_trades: int = 0
        self.losing_trades: int = 0
        self.total_volume: Decimal = Decimal('0')
        self.highest_equity: Decimal = Decimal('0')
        self.highest_unrealized_pnl: Decimal = Decimal('0')
        self.max_drawdown: Decimal = Decimal('0')

        # Position tracking
        self.positions_ref: Dict[Tuple[str, str], Position] = {}  # Reference to strategy positions
        self.closed_positions: List[Dict] = []

        # Alert management
        self.alerts: List[Alert] = []
        self._last_alert_time: Dict[AlertType, datetime] = {}

        # Risk thresholds
        self.drawdown_warning: Decimal = Decimal('0.1')     # 10% drawdown warning
        self.drawdown_critical: Decimal = Decimal('0.2')    # 20% drawdown critical
        self.margin_warning: Decimal = Decimal('0.7')       # 70% margin usage warning
        self.margin_critical: Decimal = Decimal('0.8')      # 80% margin usage critical

        # Logging intervals
        self._last_metrics_log = datetime.now()
        self._metrics_interval = timedelta(minutes=5)  # 5 minutes between logs
        self._last_position_check = datetime.now()
        self._position_check_interval = timedelta(minutes=1)  # 1 minute between position checks

    @property
    def win_rate(self) -> float:
        """Calculate win rate percentage"""
        return (self.winning_trades / self.total_trades * 100) if self.total_trades > 0 else 0

    def set_risk_thresholds(
        self,
        drawdown_warning: float = 0.1,
        drawdown_critical: float = 0.2,
        margin_warning: float = 0.7,
        margin_critical: float = 0.8
    ):
        """Set risk thresholds for alerts"""
        self.drawdown_warning = Decimal(str(drawdown_warning))
        self.drawdown_critical = Decimal(str(drawdown_critical))
        self.margin_warning = Decimal(str(margin_warning))
        self.margin_critical = Decimal(str(margin_critical))

    def set_positions_reference(self, positions: Dict[Tuple[str, str], Position]):
        """
        Set reference to strategy's positions dictionary
        This ensures positions are always up-to-date
        """
        self.positions_ref = positions

    def add_closed_position(self, position_data: Dict):
        """Track closed position history"""
        position_data["close_time"] = datetime.now()
        self.closed_positions.append(position_data)

        # Keep only last 50 closed positions
        if len(self.closed_positions) > 50:
            self.closed_positions.pop(0)


    ####################################
    ### Update and Calculate Metrics ###
    ####################################

    def process_position_update(self, position: Position) -> List[Alert]:
        """Process position update and check for risks"""
        alerts = []
        now = datetime.now()

        # Calculate unrealized PnL and update high watermark
        total_pnl = self.calculate_total_unrealized_pnl()
        if total_pnl > self.highest_unrealized_pnl:
            self.highest_unrealized_pnl = total_pnl

        # Check for drawdown
        if self.highest_unrealized_pnl > 0:
            drawdown = max(Decimal("0"), (self.highest_unrealized_pnl - total_pnl) / self.highest_unrealized_pnl)
            self.max_drawdown = max(self.max_drawdown, drawdown)

            # Alert on significant drawdown
            if drawdown > self.drawdown_critical:
                if self._should_alert(AlertType.DRAWDOWN, now):
                    alerts.append(Alert(
                        AlertType.DRAWDOWN,
                        AlertSeverity.CRITICAL,
                        f"Critical drawdown of {float(drawdown * 100):.2f}% detected"
                    ))
            elif drawdown > self.drawdown_warning:
                if self._should_alert(AlertType.DRAWDOWN, now):
                    alerts.append(Alert(
                        AlertType.DRAWDOWN,
                        AlertSeverity.WARNING,
                        f"High drawdown of {float(drawdown * 100):.2f}% detected"
                    ))

        # Check margin ratio
        if position.margin_ratio:
            if position.margin_ratio > self.margin_critical:
                if self._should_alert(AlertType.MARGIN, now):
                    alerts.append(Alert(
                        AlertType.MARGIN,
                        AlertSeverity.CRITICAL,
                        f"Critical margin usage of {float(position.margin_ratio * 100):.2f}% in {position.market_id}"
                    ))
            elif position.margin_ratio > self.margin_warning:
                if self._should_alert(AlertType.MARGIN, now):
                    alerts.append(Alert(
                        AlertType.MARGIN,
                        AlertSeverity.WARNING,
                        f"High margin usage of {float(position.margin_ratio * 100):.2f}% in {position.market_id}"
                    ))

        # Check liquidation risk
        if position.mark_price and position.liquidation_price:
            price_distance = abs(position.mark_price - position.liquidation_price) / position.mark_price
            if price_distance < Decimal("0.05"):  # Within 5% of liquidation
                if self._should_alert(AlertType.LIQUIDATION, now):
                    alerts.append(Alert(
                        AlertType.LIQUIDATION,
                        AlertSeverity.CRITICAL,
                        f"Position within {float(price_distance * 100):.2f}% of liquidation price in {position.market_id}"
                    ))

        # Add alerts to history
        for alert in alerts:
            self.alerts.append(alert)
            self._log_alert(alert)

        return alerts

    def process_trade(self, trade: Order, order: Order) -> List[Alert]:
        """Process trade and update metrics"""
        alerts = []

        # Update basic trade metrics
        self.total_trades += 1
        self.total_volume += trade.price * trade.price

        # Determine if winning trade using side and price movement

        side = trade.order_side
        exec_price = trade.price
        order_price = order.price

        is_winning = (side == Side.BUY and exec_price < order_price) or (side == Side.SELL and exec_price > order_price)
        if is_winning:
            self.winning_trades += 1
        else:
            self.losing_trades += 1

        # Check position state after trade
        total_pnl = self.calculate_total_unrealized_pnl()
        if total_pnl > self.highest_unrealized_pnl:
            self.highest_unrealized_pnl = total_pnl

        # Check drawdown
        if self.highest_unrealized_pnl > 0:
            drawdown = max(Decimal("0"), (self.highest_unrealized_pnl - total_pnl) / self.highest_unrealized_pnl)
            if drawdown > self.max_drawdown:
                self.max_drawdown = drawdown

            # Alert on drawdown after trade
            if drawdown > self.drawdown_critical:
                alerts.append(Alert(
                    AlertType.DRAWDOWN,
                    AlertSeverity.CRITICAL,
                    f"Critical drawdown of {float(drawdown * 100):.2f}% after trade"
                ))
            elif drawdown > self.drawdown_warning:
                alerts.append(Alert(
                    AlertType.DRAWDOWN,
                    AlertSeverity.WARNING,
                    f"High drawdown of {float(drawdown * 100):.2f}% after trade"
                ))

        # Add alerts to history
        for alert in alerts:
            self.alerts.append(alert)
            self._log_alert(alert)

        return alerts

    def calculate_total_unrealized_pnl(self) -> Decimal:
        """Calculate total unrealized PnL across all positions"""
        if not self.positions_ref:
            return Decimal("0")

        return sum(position.unrealized_pnl for position in self.positions_ref.values())

    def update_position_mark_prices(self, oracle_prices: Dict[str, Decimal]):
        """Update position mark prices from oracle data"""
        if not self.positions_ref:
            return

        for position in self.positions_ref.values():
            if position.market_id in oracle_prices:
                position.update(mark_price=oracle_prices[position.market_id])
                position._update_unrealized_pnl(oracle_prices[position.market_id])

    def check_all_positions(self) -> List[Alert]:
        """Check all positions for risk conditions"""
        now = datetime.now()
        if now - self._last_position_check < self._position_check_interval:
            return []

        self._last_position_check = now
        all_alerts = []

        for position in self.positions_ref.values():
            alerts = self.process_position_update(position)
            all_alerts.extend(alerts)

        return all_alerts


    #######################
    ### Log Information ###
    #######################

    def should_log_metrics(self) -> bool:
        """Check if it's time to log metrics"""
        now = datetime.now()
        if now - self._last_metrics_log > self._metrics_interval:
            self._last_metrics_log = now
            return True
        return False

    def _should_alert(self, alert_type: AlertType, now: datetime) -> bool:
        """Check if enough time has passed since last alert"""
        if alert_type not in self._last_alert_time:
            self._last_alert_time[alert_type] = now
            return True

        # Throttle alerts based on type
        throttle_intervals = {
            AlertType.MARGIN: timedelta(minutes=5),
            AlertType.LIQUIDATION: timedelta(minutes=3),
            AlertType.DRAWDOWN: timedelta(minutes=10),
            AlertType.BALANCE: timedelta(minutes=15),
        }

        min_interval = throttle_intervals.get(alert_type, timedelta(minutes=5))
        if now - self._last_alert_time[alert_type] > min_interval:
            self._last_alert_time[alert_type] = now
            return True

        return False

    def _log_alert(self, alert: Alert):
        """Log alert with appropriate severity"""
        if alert.severity == AlertSeverity.CRITICAL:
            self.logger.critical(f"🚨 CRITICAL: {alert.message}")
        elif alert.severity == AlertSeverity.WARNING:
            self.logger.warning(f"⚠️ WARNING: {alert.message}")
        else:
            self.logger.info(f"ℹ️ INFO: {alert.message}")

    def add_custom_alert(self, alert_type: AlertType, severity: AlertSeverity, message: str, data: Optional[Dict] = None):
        """Add a custom alert"""
        alert = Alert(
            type=alert_type,
            severity=severity,
            message=message,
            data=data or {}
        )
        self.alerts.append(alert)
        self._log_alert(alert)
        return alert

    def get_performance_summary(self) -> str:
        """Generate formatted performance report"""
        # Core metrics
        win_rate = (self.winning_trades / self.total_trades * 100) if self.total_trades > 0 else 0
        total_pnl = self.calculate_total_unrealized_pnl()

        metrics_data = {
            "Metric": [
                "Total Trades",
                "Win Rate",
                "Total Volume",
                "Unrealized PnL",
                "Max Drawdown",
                "Active Positions",
                "Running Time"
            ],
            "Value": [
                f"{self.total_trades} ({self.winning_trades} wins, {self.losing_trades} losses)",
                f"{win_rate:.2f}%",
                f"{float(self.total_volume):.2f}",
                f"{float(total_pnl):.4f}",
                f"{float(self.max_drawdown * 100):.2f}%",
                len(self.positions_ref),
                str(datetime.now() - self.start_time).split('.')[0]
            ]
        }
        metrics_df = pd.DataFrame(metrics_data)

        # Position summary
        position_data = []
        for pos in self.positions_ref.values():
            position_data.append({
                "Market": pos.market_id[:8] + "...",
                "Size": f"{float(pos.quantity):.4f}",
                "Entry": f"{float(pos.entry_price):.4f}",
                "Mark": f"{float(pos.mark_price):.4f}" if pos.mark_price else "N/A",
                "PnL": f"{float(pos.unrealized_pnl):.4f}",
                "Margin%": f"{float(pos.margin_ratio * 100):.2f}%" if pos.margin_ratio else "N/A",
            })
        position_df = pd.DataFrame(position_data) if position_data else None

        # Recent alerts
        recent_alerts = self.alerts[-5:] if self.alerts else []  # Last 5 alerts
        alert_data = {
            "Time": [a.timestamp.strftime("%H:%M:%S") for a in recent_alerts],
            "Type": [a.type.name for a in recent_alerts],
            "Severity": [a.severity.name for a in recent_alerts],
            "Message": [a.message for a in recent_alerts]
        }
        alert_df = pd.DataFrame(alert_data) if recent_alerts else None

        # Build formatted report
        report = [
            "\n" + "="*80 + "\n",
            "=== Strategy Performance Report ===\n",
            tabulate(metrics_df, headers='keys', tablefmt='fancy_grid', showindex=False),
            "\n=== Active Positions ===\n",
            tabulate(position_df, headers='keys', tablefmt='simple', showindex=False) if position_df is not None else "No active positions\n",
            "\n=== Recent Alerts ===\n",
            tabulate(alert_df, headers='keys', tablefmt='simple', showindex=False) if alert_df is not None else "No recent alerts\n",
            "="*80 + "\n"
        ]

        return "\n".join(report)

    def log_all_metrics(self, force: bool = False):
        """Log complete performance metrics"""
        if not force and not self.should_log_metrics():
            return

        try:
            performance_summary = self.get_performance_summary()
            self.logger.info(performance_summary)
            self._last_metrics_log = datetime.now()
        except Exception as e:
            self.logger.error(f"Error logging metrics: {e}")

    def add_alert(self, alert: Alert):
        """Add custom alert to history"""
        #alert = Alert(
        #    type=alert_type,
        #    severity=severity,
        #    message=message,
        #    data=data or {}
        #)
        self.alerts.append(alert)

        # Log alert based on severity
        if alert.severity == AlertSeverity.CRITICAL:
            self.logger.critical(f"🚨 ALERT: {alert.message}")
        elif alert.severity == AlertSeverity.WARNING:
            self.logger.warning(f"⚠️ WARNING: {alert.message}")
        else:
            self.logger.info(f"ℹ️ INFO: {alert.message}")

    def to_dataframe(self) -> pd.DataFrame:
        """Convert metrics to pandas DataFrame for pretty printing"""
        data = {
            'Metric': [
                'Total Trades',
                'Win Rate',
                'Total Volume',
                'Realized PnL',
                'Unrealized PnL',
                'Total PnL',
                'Max Drawdown',
                'Active Positions',
                'Running Time',
                'Hourly PnL Avg',
                'Highest Equity'
            ],
            'Value': [
                f"{self.total_trades} ({self.winning_trades} wins, {self.losing_trades} losses)",
                f"{self.win_rate:.2f}%",
                f"{self.total_volume:.2f}",
                f"{self.realized_pnl:.2f}",
                f"{self.unrealized_pnl:.2f}",
                f"{self.total_pnl:.2f}",
                f"{float(self.max_drawdown * 100):.2f}%",
                len(self.current_positions),
                str(datetime.now() - self.start_time).split('.')[0],
                f"{sum(self.hourly_pnl) / len(self.hourly_pnl):.2f}" if self.hourly_pnl else "0.00",
                f"{self.highest_equity:.2f}"
            ]
        }
        return pd.DataFrame(data)

    def get_formatted_table(self) -> str:
        """Get nicely formatted performance table"""
        df = self.to_dataframe()
        position_summary = self._format_position_summary()

        return (
            "\n=== Strategy Performance Report ===\n" +
            tabulate(df, headers='keys', tablefmt='fancy_grid', showindex=False) +
            "\n=== Active Positions ===\n" +
            position_summary +
            "\n=== Recent Alerts ===\n" +
            self._format_recent_alerts()
        )

    def _format_position_summary(self) -> str:
        """Format current positions summary"""
        if not self.current_positions_ref:
            return "No active positions\n"

        position_data = {
            'Market': [],
            'Size': [],
            'Entry': [],
            'Current': [],
            'PnL': []
        }

        for market_id, pos in self.current_positions.items():
            if Decimal(str(pos.get('quantity', '0'))) == 0:
                continue

            position_data['Market'].append(market_id[:8] + '...')
            position_data['Size'].append(f"{float(pos['quantity']):.4f}")
            position_data['Entry'].append(f"{float(pos['entry_price']):.4f}")
            mark_price = pos.get('mark_price', pos.get('entry_price', 0))
            position_data['Current'].append(f"{float(mark_price):.4f}")

            # Calculate position PnL
            size = Decimal(str(pos['quantity']))
            entry = Decimal(str(pos['entry_price']))
            current = Decimal(str(mark_price))
            pnl = (current - entry) * size if pos.get('is_long', False) else (entry - current) * size
            position_data['PnL'].append(f"{float(pnl):.2f}")

        df = pd.DataFrame(position_data)
        return tabulate(df, headers='keys', tablefmt='simple', showindex=False)

    def _format_recent_alerts(self) -> str:
        """Format recent alerts for display"""
        recent_alerts = [
            alert for alert in self.alerts
            if (datetime.now() - alert.timestamp) < timedelta(hours=1)
        ]
        if not recent_alerts:
            return "No recent alerts\n"

        alert_data = {
            'Time': [a.timestamp.strftime('%H:%M:%S') for a in recent_alerts],
            'Type': [a.type.name for a in recent_alerts],
            'Severity': [a.severity.name for a in recent_alerts],
            'Message': [a.message for a in recent_alerts]
        }
        df = pd.DataFrame(alert_data)
        return tabulate(df, headers='keys', tablefmt='simple', showindex=False)

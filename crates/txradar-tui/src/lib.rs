//! Radar TUI (Phase 7) — TxRadar's "mission control" dashboard.
//!
//! Renders, in real time:
//! * Header: connection state, current slot, next Jito leader window.
//! * Tip oracle panel: the live band (low/mid/high + basis percentile) and the
//!   congestion proxy the agent reasons over.
//! * Attempts table: per-attempt lifecycle with a latency waterfall across
//!   Submitted -> Processed -> Confirmed -> Finalized, tip paid, landed slot,
//!   and failure/fault markers.
//! * Agent reasoning feed: the decision rationale stream, so "reasoning is
//!   visible" (a judging criterion).
//!
//! The crate is split cleanly: [`DashboardState`] is a passive view-model the
//! orchestrator updates from stream/agent/tracker events, and [`draw`] is a pure
//! function of that state (unit-tested against a `TestBackend`). Terminal
//! lifecycle helpers ([`enter`]/[`restore`]) wrap raw-mode + alternate-screen.

use std::io::{self, Stdout};

use crossterm::{
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, List, ListItem, Paragraph, Row, Table},
    Frame, Terminal,
};

/// The live tip band the agent reasons over, flattened for display.
#[derive(Debug, Clone)]
pub struct TipBandView {
    pub low: u64,
    pub mid: u64,
    pub high: u64,
    pub basis: String,
    pub skip_rate: f32,
}

/// One attempt's row in the lifecycle table.
#[derive(Debug, Clone)]
pub struct AttemptRow {
    pub attempt_id: u64,
    pub tip_lamports: u64,
    /// "submitted" | "processed" | "confirmed" | "finalized" | "failed".
    pub stage: String,
    pub landed_slot: Option<u64>,
    pub submit_to_processed_ms: Option<i64>,
    pub processed_to_confirmed_ms: Option<i64>,
    pub confirmed_to_finalized_ms: Option<i64>,
    pub failure: Option<String>,
    pub fault_injected: bool,
}

/// Snapshot of everything the dashboard draws on a given frame. The orchestrator
/// updates this from stream/agent/tracker events; the render loop reads it.
#[derive(Debug, Default)]
pub struct DashboardState {
    pub current_slot: u64,
    pub connection: String,
    pub next_leader_slot: Option<u64>,
    pub tip: Option<TipBandView>,
    pub attempts: Vec<AttemptRow>,
    /// Agent rationale feed, newest last.
    pub reasoning: Vec<String>,
    /// One-line status shown in the footer.
    pub status_line: String,
    /// Network label (e.g. "testnet-sim" / "mainnet").
    pub network: String,
}

const MAX_REASONING: usize = 100;

impl DashboardState {
    pub fn new(network: impl Into<String>) -> Self {
        Self { network: network.into(), connection: "connecting".into(), ..Default::default() }
    }

    /// Append an agent rationale line (capped to the most recent entries).
    pub fn push_reasoning(&mut self, line: impl Into<String>) {
        self.reasoning.push(line.into());
        let len = self.reasoning.len();
        if len > MAX_REASONING {
            self.reasoning.drain(0..len - MAX_REASONING);
        }
    }

    /// Insert or replace an attempt row, keyed by `attempt_id`.
    pub fn upsert_attempt(&mut self, row: AttemptRow) {
        match self.attempts.iter_mut().find(|r| r.attempt_id == row.attempt_id) {
            Some(existing) => *existing = row,
            None => self.attempts.push(row),
        }
    }
}

/// Terminal type used by the dashboard.
pub type Tui = Terminal<CrosstermBackend<Stdout>>;

/// Enter raw mode + the alternate screen and build a terminal.
pub fn enter() -> io::Result<Tui> {
    enable_raw_mode()?;
    let mut out = io::stdout();
    execute!(out, EnterAlternateScreen)?;
    Terminal::new(CrosstermBackend::new(out))
}

/// Restore the terminal to its normal state. Safe to call more than once.
pub fn restore(term: &mut Tui) -> io::Result<()> {
    disable_raw_mode()?;
    execute!(term.backend_mut(), LeaveAlternateScreen)?;
    term.show_cursor()
}

/// Render one frame from `state`. Pure function of the state — no I/O, no
/// mutation — so it can be unit-tested against a `TestBackend`.
pub fn draw(f: &mut Frame, state: &DashboardState) {
    let area = f.area();
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // header
            Constraint::Min(8),    // body (tips | attempts)
            Constraint::Length(8), // reasoning feed
            Constraint::Length(1), // footer
        ])
        .split(area);

    draw_header(f, rows[0], state);

    let body = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Length(34), Constraint::Min(40)])
        .split(rows[1]);
    draw_tips(f, body[0], state);
    draw_attempts(f, body[1], state);

    draw_reasoning(f, rows[2], state);
    draw_footer(f, rows[3], state);
}

fn draw_header(f: &mut Frame, area: Rect, state: &DashboardState) {
    let leader = state
        .next_leader_slot
        .map(|s| format!("next leader @ slot {s}"))
        .unwrap_or_else(|| "next leader: —".into());
    let conn_color = match state.connection.as_str() {
        "connected" => Color::Green,
        "connecting" | "simulated" => Color::Yellow,
        _ => Color::Red,
    };
    let line = Line::from(vec![
        Span::styled("TxRadar", Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
        Span::raw("  "),
        Span::styled(format!("[{}]", state.network), Style::default().fg(Color::Magenta)),
        Span::raw("  slot "),
        Span::styled(state.current_slot.to_string(), Style::default().fg(Color::White).add_modifier(Modifier::BOLD)),
        Span::raw("  "),
        Span::styled(state.connection.clone(), Style::default().fg(conn_color)),
        Span::raw("  "),
        Span::styled(leader, Style::default().fg(Color::DarkGray)),
    ]);
    f.render_widget(
        Paragraph::new(line).block(Block::default().borders(Borders::ALL).title(" radar ")),
        area,
    );
}

fn draw_tips(f: &mut Frame, area: Rect, state: &DashboardState) {
    let block = Block::default().borders(Borders::ALL).title(" tip oracle ");
    let body = match &state.tip {
        None => vec![Line::from(Span::styled("awaiting floor…", Style::default().fg(Color::DarkGray)))],
        Some(t) => vec![
            Line::from(vec![
                Span::styled("anchor ", Style::default().fg(Color::Gray)),
                Span::styled(t.basis.clone(), Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
            ]),
            Line::from(format!("low   {:>10} lamports", t.low)),
            Line::from(vec![
                Span::raw("mid   "),
                Span::styled(format!("{:>10}", t.mid), Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)),
                Span::raw(" lamports"),
            ]),
            Line::from(format!("high  {:>10} lamports", t.high)),
            Line::from(""),
            Line::from(format!("skip rate  {:>5.1}%", t.skip_rate * 100.0)),
        ],
    };
    f.render_widget(Paragraph::new(body).block(block), area);
}

fn draw_attempts(f: &mut Frame, area: Rect, state: &DashboardState) {
    let header = Row::new(vec!["#", "tip", "stage", "slot", "->proc", "->conf", "->final", "note"])
        .style(Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD));

    let ms = |v: Option<i64>| v.map(|n| format!("{n}ms")).unwrap_or_else(|| "—".into());
    let rows = state.attempts.iter().map(|a| {
        let (stage_txt, stage_color) = match a.stage.as_str() {
            "finalized" => ("finalized", Color::Green),
            "confirmed" => ("confirmed", Color::LightGreen),
            "processed" => ("processed", Color::Yellow),
            "failed" => ("failed", Color::Red),
            other => (other, Color::Gray),
        };
        let note = match (&a.failure, a.fault_injected) {
            (Some(fc), true) => format!("injected: {fc}"),
            (Some(fc), false) => fc.clone(),
            (None, true) => "fault-injected".into(),
            (None, false) => String::new(),
        };
        Row::new(vec![
            Cell::from(a.attempt_id.to_string()),
            Cell::from(a.tip_lamports.to_string()),
            Cell::from(Span::styled(stage_txt.to_string(), Style::default().fg(stage_color))),
            Cell::from(a.landed_slot.map(|s| s.to_string()).unwrap_or_else(|| "—".into())),
            Cell::from(ms(a.submit_to_processed_ms)),
            Cell::from(ms(a.processed_to_confirmed_ms)),
            Cell::from(ms(a.confirmed_to_finalized_ms)),
            Cell::from(note),
        ])
    });

    let widths = [
        Constraint::Length(3),
        Constraint::Length(8),
        Constraint::Length(10),
        Constraint::Length(12),
        Constraint::Length(7),
        Constraint::Length(7),
        Constraint::Length(8),
        Constraint::Min(10),
    ];
    let table = Table::new(rows, widths)
        .header(header)
        .block(Block::default().borders(Borders::ALL).title(" bundle lifecycle "));
    f.render_widget(table, area);
}

fn draw_reasoning(f: &mut Frame, area: Rect, state: &DashboardState) {
    let height = area.height.saturating_sub(2) as usize; // borders
    let start = state.reasoning.len().saturating_sub(height.max(1));
    let items: Vec<ListItem> = state.reasoning[start..]
        .iter()
        .map(|r| ListItem::new(Line::from(vec![
            Span::styled("> ", Style::default().fg(Color::Magenta)),
            Span::raw(r.clone()),
        ])))
        .collect();
    f.render_widget(
        List::new(items).block(Block::default().borders(Borders::ALL).title(" agent reasoning ")),
        area,
    );
}

fn draw_footer(f: &mut Frame, area: Rect, state: &DashboardState) {
    let txt = if state.status_line.is_empty() {
        "q: quit".to_string()
    } else {
        format!("{}   ·   q: quit", state.status_line)
    };
    f.render_widget(
        Paragraph::new(Span::styled(txt, Style::default().fg(Color::DarkGray))),
        area,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::backend::TestBackend;

    fn sample_state() -> DashboardState {
        let mut s = DashboardState::new("testnet-sim");
        s.connection = "connected".into();
        s.current_slot = 300_000_005;
        s.next_leader_slot = Some(300_000_012);
        s.tip = Some(TipBandView { low: 1_000, mid: 5_000, high: 30_000, basis: "p50".into(), skip_rate: 0.12 });
        s.upsert_attempt(AttemptRow {
            attempt_id: 1,
            tip_lamports: 1_548,
            stage: "failed".into(),
            landed_slot: None,
            submit_to_processed_ms: None,
            processed_to_confirmed_ms: None,
            confirmed_to_finalized_ms: None,
            failure: Some("expired_blockhash".into()),
            fault_injected: true,
        });
        s.upsert_attempt(AttemptRow {
            attempt_id: 2,
            tip_lamports: 12_000,
            stage: "finalized".into(),
            landed_slot: Some(300_000_005),
            submit_to_processed_ms: Some(421),
            processed_to_confirmed_ms: Some(480),
            confirmed_to_finalized_ms: Some(12_100),
            failure: None,
            fault_injected: false,
        });
        s.push_reasoning("submit at oracle p50 (1548 lamports); calm market");
        s.push_reasoning("blockhash expired -> refresh + raise tip to 12000, resubmit");
        s.status_line = "SUCCESS — landed after autonomous recovery".into();
        s
    }

    fn rendered_text(width: u16, height: u16, state: &DashboardState) -> String {
        let backend = TestBackend::new(width, height);
        let mut term = Terminal::new(backend).unwrap();
        term.draw(|f| draw(f, state)).unwrap();
        let buf = term.backend().buffer().clone();
        buf.content().iter().map(|c| c.symbol().to_string()).collect()
    }

    #[test]
    fn renders_key_panels_without_panic() {
        let text = rendered_text(120, 30, &sample_state());
        assert!(text.contains("TxRadar"));
        assert!(text.contains("tip oracle"));
        assert!(text.contains("bundle lifecycle"));
        assert!(text.contains("agent reasoning"));
    }

    #[test]
    fn surfaces_fault_and_recovery() {
        let text = rendered_text(120, 30, &sample_state());
        // The injected fault and the agent's recovery rationale are both visible.
        assert!(text.contains("injected"));
        assert!(text.contains("finalized"));
        assert!(text.contains("resubmit"));
    }

    #[test]
    fn upsert_replaces_same_attempt() {
        let mut s = DashboardState::new("t");
        s.upsert_attempt(AttemptRow {
            attempt_id: 1, tip_lamports: 1, stage: "submitted".into(), landed_slot: None,
            submit_to_processed_ms: None, processed_to_confirmed_ms: None,
            confirmed_to_finalized_ms: None, failure: None, fault_injected: false,
        });
        s.upsert_attempt(AttemptRow {
            attempt_id: 1, tip_lamports: 2, stage: "finalized".into(), landed_slot: Some(9),
            submit_to_processed_ms: Some(10), processed_to_confirmed_ms: None,
            confirmed_to_finalized_ms: None, failure: None, fault_injected: false,
        });
        assert_eq!(s.attempts.len(), 1);
        assert_eq!(s.attempts[0].stage, "finalized");
        assert_eq!(s.attempts[0].tip_lamports, 2);
    }

    #[test]
    fn tiny_terminal_does_not_panic() {
        // Defensive: very small areas must not panic the layout.
        let _ = rendered_text(20, 10, &sample_state());
    }
}

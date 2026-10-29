//! SQL front-end: lexer, AST, recursive-descent parser.
//! Surface: CREATE/DROP TABLE, INSERT (+RETURNING), SELECT (expressions,
//! FROM with JOINs, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT/OFFSET),
//! UPDATE/DELETE (+RETURNING), BEGIN/COMMIT/ROLLBACK (v1: per-statement).

use crate::engine::{AggKind, Ty, Value};

// ------------------------------------------------------------------- lexer --

#[derive(Clone, Debug)]
pub enum Tok {
    Ident(String),  // case-folded to lowercase
    Num(String),
    Str(String),
    Op(&'static str),
    Param(u32), // $1
    Eof,
}

pub fn lex(src: &str) -> Result<Vec<Tok>, String> {
    let b = src.as_bytes();
    let mut i = 0usize;
    let mut out = Vec::new();
    while i < b.len() {
        let c = b[i];
        match c {
            b' ' | b'\t' | b'\r' | b'\n' => i += 1,
            b'-' if i + 1 < b.len() && b[i + 1] == b'-' => {
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if i + 1 < b.len() && b[i + 1] == b'*' => {
                i += 2;
                while i + 1 < b.len() && !(b[i] == b'*' && b[i + 1] == b'/') {
                    i += 1;
                }
                if i + 1 >= b.len() {
                    return Err("unterminated block comment".into());
                }
                i += 2;
            }
            b'\'' => {
                i += 1;
                let mut s = Vec::new();
                loop {
                    if i >= b.len() {
                        return Err("unterminated string literal".into());
                    }
                    if b[i] == b'\'' {
                        if i + 1 < b.len() && b[i + 1] == b'\'' {
                            s.push(b'\'');
                            i += 2;
                        } else {
                            i += 1;
                            break;
                        }
                    } else {
                        s.push(b[i]);
                        i += 1;
                    }
                }
                out.push(Tok::Str(String::from_utf8_lossy(&s).into_owned()));
            }
            b'"' => {
                i += 1;
                let mut s = Vec::new();
                loop {
                    if i >= b.len() {
                        return Err("unterminated quoted identifier".into());
                    }
                    if b[i] == b'"' {
                        i += 1;
                        break;
                    }
                    s.push(b[i]);
                    i += 1;
                }
                out.push(Tok::Ident(String::from_utf8_lossy(&s).into_owned()));
            }
            b'$' => {
                let start = i + 1;
                i += 1;
                while i < b.len() && b[i].is_ascii_digit() {
                    i += 1;
                }
                if start == i {
                    return Err("bad parameter reference".into());
                }
                let n: u32 = std::str::from_utf8(&b[start..i])
                    .unwrap()
                    .parse()
                    .map_err(|_| "bad parameter reference".to_string())?;
                out.push(Tok::Param(n));
            }
            b'0'..=b'9' => {
                let start = i;
                while i < b.len() && b[i].is_ascii_digit() {
                    i += 1;
                }
                if i + 1 < b.len() && b[i] == b'.' && b[i + 1].is_ascii_digit() {
                    i += 1;
                    while i < b.len() && b[i].is_ascii_digit() {
                        i += 1;
                    }
                }
                if i < b.len() && (b[i] == b'e' || b[i] == b'E') {
                    let save = i;
                    i += 1;
                    if i < b.len() && (b[i] == b'+' || b[i] == b'-') {
                        i += 1;
                    }
                    if i < b.len() && b[i].is_ascii_digit() {
                        while i < b.len() && b[i].is_ascii_digit() {
                            i += 1;
                        }
                    } else {
                        i = save;
                    }
                }
                out.push(Tok::Num(
                    std::str::from_utf8(&b[start..i]).unwrap().to_string(),
                ));
            }
            _ if c.is_ascii_alphabetic() || c == b'_' => {
                let start = i;
                while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_' || b[i] == b'$')
                {
                    i += 1;
                }
                // E'...' escape-string literal (psql emits E'\n' etc.)
                if b[start..i].eq_ignore_ascii_case(b"e") && i < b.len() && b[i] == b'\'' {
                    i += 1; // opening quote
                    let mut s = Vec::new();
                    loop {
                        if i >= b.len() {
                            return Err("unterminated string literal".into());
                        }
                        match b[i] {
                            b'\'' => {
                                if i + 1 < b.len() && b[i + 1] == b'\'' {
                                    s.push(b'\'');
                                    i += 2;
                                } else {
                                    i += 1;
                                    break;
                                }
                            }
                            b'\\' if i + 1 < b.len() => {
                                i += 1;
                                match b[i] {
                                    b'n' => s.push(b'\n'),
                                    b't' => s.push(b'\t'),
                                    b'r' => s.push(b'\r'),
                                    b'\\' => s.push(b'\\'),
                                    b'\'' => s.push(b'\''),
                                    b'b' => s.push(8),
                                    b'f' => s.push(12),
                                    b'v' => s.push(11),
                                    b'0' => s.push(0),
                                    other => {
                                        return Err(format!(
                                            "unsupported escape \\{}",
                                            other as char
                                        ))
                                    }
                                }
                                i += 1;
                            }
                            other => {
                                s.push(other);
                                i += 1;
                            }
                        }
                    }
                    out.push(Tok::Str(String::from_utf8_lossy(&s).into_owned()));
                } else {
                    out.push(Tok::Ident(
                        std::str::from_utf8(&b[start..i]).unwrap().to_lowercase(),
                    ));
                }
            }
            b'(' | b')' | b',' | b';' | b'.' | b'+' | b'-' | b'*' | b'/' | b'%' | b'=' | b'['
            | b']' => {
                const SINGLE: &[(u8, &str)] = &[
                    (b'(', "("),
                    (b')', ")"),
                    (b',', ","),
                    (b';', ";"),
                    (b'.', "."),
                    (b'+', "+"),
                    (b'-', "-"),
                    (b'*', "*"),
                    (b'/', "/"),
                    (b'[', "["),
                    (b']', "]"),
                    (b'%', "%"),
                    (b'=', "="),
                ];
                let op = SINGLE.iter().find(|(c0, _)| *c0 == c).unwrap().1;
                out.push(Tok::Op(op));
                i += 1;
            }
            b'<' => {
                if i + 1 < b.len() && b[i + 1] == b'=' {
                    out.push(Tok::Op("<="));
                    i += 2;
                } else if i + 1 < b.len() && b[i + 1] == b'>' {
                    out.push(Tok::Op("<>"));
                    i += 2;
                } else {
                    out.push(Tok::Op("<"));
                    i += 1;
                }
            }
            b'>' => {
                if i + 1 < b.len() && b[i + 1] == b'=' {
                    out.push(Tok::Op(">="));
                    i += 2;
                } else {
                    out.push(Tok::Op(">"));
                    i += 1;
                }
            }
            b'!' if i + 1 < b.len() && b[i + 1] == b'=' => {
                out.push(Tok::Op("!="));
                i += 2;
            }
            b'!' if i + 1 < b.len() && b[i + 1] == b'~' => {
                out.push(Tok::Op("!~"));
                i += 2;
            }
            b'~' => {
                out.push(Tok::Op("~"));
                i += 1;
            }
            b':' if i + 1 < b.len() && b[i + 1] == b':' => {
                out.push(Tok::Op("::"));
                i += 2;
            }
            b'|' if i + 1 < b.len() && b[i + 1] == b'|' => {
                out.push(Tok::Op("||"));
                i += 2;
            }
            _ => return Err(format!("unexpected character '{}'", c as char)),
        }
    }
    out.push(Tok::Eof);
    Ok(out)
}

// --------------------------------------------------------------------- AST --

#[derive(Clone, Debug, PartialEq)]
pub enum Expr {
    Lit(Value),
    Col(String),
    QCol(String, String), // table (alias), column
    Param(i32),           // $1
    Neg(Box<Expr>),
    Not(Box<Expr>),
    Bin(Box<Expr>, &'static str, Box<Expr>),
    IsNull(Box<Expr>, bool), // IS [NOT] NULL
    Like(Box<Expr>, Box<Expr>, bool),
    InList(Box<Expr>, Vec<Expr>, bool),
    /// IN (SELECT ...): the subquery is evaluated eagerly at rewrite time
    /// (uncorrelated only) and lowered to an InList of literals
    InSub(Box<Expr>, Box<SelectStmt>, bool),
    /// Window function call: fn(args) OVER (spec). Replaced with
    /// Col("__win_k") before evaluation; the values are computed by the
    /// window pass in the reference executor.
    Win {
        kind: WinKind,
        args: Vec<Expr>,
        spec: Box<WindowSpec>,
        filter: Option<Box<Expr>>, // FILTER (WHERE ...) on a window aggregate
    },
    Between(Box<Expr>, Box<Expr>, Box<Expr>, bool),
    Case {
        operand: Option<Box<Expr>>,
        when_then: Vec<(Expr, Expr)>,
        else_: Option<Box<Expr>>,
    },
    Cast(Box<Expr>, Ty),
    Func(String, Vec<Expr>),
    Agg(AggKind, Box<Expr>, bool, Option<Box<Expr>>), // (kind, arg, distinct, FILTER)
    AggRef(usize),                 // filled during aggregate evaluation
    Subquery(Box<SelectStmt>),     // scalar subquery (evaluated eagerly)
}

impl Expr {
    /// True if this expression contains an aggregate call.
    pub fn has_agg(&self) -> bool {
        match self {
            Expr::Agg(..) => true,
            Expr::Neg(a) | Expr::Not(a) | Expr::IsNull(a, _) | Expr::Cast(a, _) => a.has_agg(),
            Expr::Like(a, b, _) | Expr::Between(a, b, _, _) => a.has_agg() || b.has_agg(),
            Expr::InList(a, l, _) => a.has_agg() || l.iter().any(|e| e.has_agg()),
            Expr::InSub(a, _, _) => a.has_agg(),
            Expr::Win { kind, args, spec, filter } => {
                // an aggregate inside OVER makes the whole query an
                // aggregate query (PG), e.g. rank() OVER (ORDER BY sum(x))
                matches!(kind, WinKind::Agg(_))
                    || args.iter().any(|a| a.has_agg())
                    || spec.partition_by.iter().any(|a| a.has_agg())
                    || spec.order_by.iter().any(|(e, _, _)| e.has_agg())
                    || filter.as_ref().is_some_and(|e| e.has_agg())
            }
            Expr::Bin(a, _, b) => a.has_agg() || b.has_agg(),
            Expr::Case {
                operand,
                when_then,
                else_,
            } => {
                operand.as_ref().is_some_and(|o| o.has_agg())
                    || when_then.iter().any(|(w, t)| w.has_agg() || t.has_agg())
                    || else_.as_ref().is_some_and(|e| e.has_agg())
            }
            Expr::Func(_, args) => args.iter().any(|a| a.has_agg()),
            _ => false,
        }
    }

    /// Highest $N parameter referenced (0 if none).
    pub fn max_param(&self) -> i32 {
        match self {
            Expr::Param(n) => *n,
            Expr::Neg(a) | Expr::Not(a) | Expr::IsNull(a, _) | Expr::Cast(a, _) => a.max_param(),
            Expr::Like(a, b, _) | Expr::Between(a, b, _, _) => {
                a.max_param().max(b.max_param())
            }
            Expr::InList(a, l, _) => {
                a.max_param().max(l.iter().map(|e| e.max_param()).max().unwrap_or(0))
            }
            // (like Subquery: params inside subqueries are not counted)
            Expr::InSub(a, _, _) => a.max_param(),
            Expr::Win { args, spec, .. } => args
                .iter()
                .map(|a| a.max_param())
                .max()
                .unwrap_or(0)
                .max(spec.partition_by.iter().map(|a| a.max_param()).max().unwrap_or(0))
                .max(spec.order_by.iter().map(|(e, _, _)| e.max_param()).max().unwrap_or(0)),
            Expr::Bin(a, _, b) => a.max_param().max(b.max_param()),
            Expr::Case {
                operand,
                when_then,
                else_,
            } => {
                let mut m = operand.as_ref().map_or(0, |o| o.max_param());
                for (w, t) in when_then {
                    m = m.max(w.max_param()).max(t.max_param());
                }
                if let Some(e) = else_ {
                    m = m.max(e.max_param());
                }
                m
            }
            Expr::Func(_, args) => args.iter().map(|a| a.max_param()).max().unwrap_or(0),
            Expr::Agg(_, a, _, f) => a.max_param().max(f.as_ref().map_or(0, |e| e.max_param())),
            _ => 0,
        }
    }

    /// Collects plain column references (excluding AggRef), without descending
    /// into anything (aggregates own their arguments).
    pub fn collect_cols<'a>(&'a self, out: &mut Vec<&'a Expr>) {
        match self {
            Expr::Col(_) | Expr::QCol(..) => out.push(self),
            Expr::Neg(a) | Expr::Not(a) | Expr::IsNull(a, _) | Expr::Cast(a, _) => {
                a.collect_cols(out)
            }
            Expr::Like(a, b, _) | Expr::Between(a, b, _, _) => {
                a.collect_cols(out);
                b.collect_cols(out);
            }
            Expr::InList(a, l, _) => {
                a.collect_cols(out);
                l.iter().for_each(|e| e.collect_cols(out));
            }
            Expr::InSub(a, _, _) => {
                a.collect_cols(out);
            }
            Expr::Win { args, spec, .. } => {
                for a in args {
                    a.collect_cols(out);
                }
                for a in &spec.partition_by {
                    a.collect_cols(out);
                }
                for (e, _, _) in &spec.order_by {
                    e.collect_cols(out);
                }
            }
            Expr::Bin(a, _, b) => {
                a.collect_cols(out);
                b.collect_cols(out);
            }
            Expr::Case {
                operand,
                when_then,
                else_,
            } => {
                if let Some(o) = operand {
                    o.collect_cols(out);
                }
                for (w, t) in when_then {
                    w.collect_cols(out);
                    t.collect_cols(out);
                }
                if let Some(e) = else_ {
                    e.collect_cols(out);
                }
            }
            Expr::Func(_, args) => args.iter().for_each(|a| a.collect_cols(out)),
            _ => {}
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum SelectItem {
    Star,
    Expr(Expr, Option<String>),
}

/// Time-travel qualifier on a FROM item (v2.0).
#[derive(Clone, Debug, PartialEq)]
pub enum AsOf {
    /// by named snapshot (created with CREATE SNAPSHOT)
    Snapshot(String),
    /// by WAL log sequence number
    Lsn(u64),
}

#[derive(Clone, Debug)]

pub struct TableRef {
    pub name: String,
    pub alias: Option<String>,
    /// AS OF time travel: None = current state
    pub as_of: Option<AsOf>,
    /// set-returning function in FROM position (generate_series): the call
    /// is kept as source text fragments to evaluate at execution time
    pub func_args: Option<Vec<Expr>>,
    /// column alias list after a table alias: FROM s(a, b) / g(x)
    pub col_aliases: Vec<String>,
    /// derived table in FROM position: FROM (SELECT ...) [AS] x [(a, b)].
    /// None everywhere else.
    pub subquery: Option<Box<Stmt>>,
}

impl PartialEq for TableRef {
    fn eq(&self, other: &Self) -> bool {
        // subquery is intentionally not compared: the only PartialEq use
        // in the engine is the GROUP BY coverage check, and expression
        // trees never embed FROM items
        self.name == other.name
            && self.alias == other.alias
            && self.as_of == other.as_of
            && self.func_args == other.func_args
            && self.col_aliases == other.col_aliases
    }
}

impl TableRef {
    pub fn effective_name(&self) -> &str {
        match &self.alias {
            Some(a) => a,
            // unaliased qualified name: scope key is the bare table part
            None => self.bare(),
        }
    }

    /// The table part of a possibly schema-qualified name
    /// ("pg_catalog.pg_class" -> "pg_class").
    pub fn bare(&self) -> &str {
        match self.name.rfind('.') {
            Some(i) => &self.name[i + 1..],
            None => &self.name,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum JoinKind {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Join {
    pub kind: JoinKind,
    pub table: TableRef,
    pub on: Option<Expr>,
}

#[derive(Clone, Debug)]
pub struct CteDef {
    pub name: String,
    pub col_aliases: Vec<String>,
    pub body: Box<Stmt>,
}

/// ON CONFLICT clause of INSERT.
#[derive(Clone, Debug)]
pub struct OnConflict {
    /// conflict target column (puddle indexes are single-column, so a
    /// target list must have exactly one entry; empty = "any index")
    pub target: Vec<String>,
    pub action: ConflictAction,
}

#[derive(Clone, Debug)]
pub enum ConflictAction {
    Nothing,
    Update {
        sets: Vec<(String, Expr)>,
        guard: Option<Expr>, // optional WHERE on the DO UPDATE
    },
}

/// Window function kinds (aggregates over frames reuse AggKind).
#[derive(Clone, Debug, PartialEq)]
pub enum WinKind {
    RowNumber,
    Rank,
    DenseRank,
    PercentRank,
    CumeDist,
    Ntile,
    Lag,
    Lead,
    FirstValue,
    LastValue,
    NthValue,
    Agg(AggKind),
}

/// OVER ( [PARTITION BY ...] [ORDER BY ...] [frame] )
#[derive(Clone, Debug, PartialEq)]
pub struct WindowSpec {
    /// OVER w / OVER (w ...): the referenced WINDOW-clause name, resolved
    /// (merged) before execution; None for inline specs.
    pub name: Option<String>,
    pub partition_by: Vec<Expr>,
    pub order_by: Vec<(Expr, bool, bool)>, // (key, desc, nulls_first)
    pub frame: Option<Frame>,
}

/// The default frame is RANGE UNBOUNDED PRECEDING .. CURRENT ROW (PG).
#[derive(Clone, Debug, PartialEq)]
pub struct Frame {
    pub rows: bool, // true = ROWS mode; false = RANGE mode
    pub start: FrameBound,
    pub end: FrameBound,
}

#[derive(Clone, Debug, PartialEq)]
pub enum FrameBound {
    UnboundedPreceding,
    Preceding(i64),
    CurrentRow,
    Following(i64),
    UnboundedFollowing,
}

#[derive(Clone, Debug, PartialEq)]
pub struct FromItem {
    pub base: TableRef,
    pub joins: Vec<Join>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SelectStmt {
    pub distinct: bool,
    /// WINDOW w AS (spec), ... clause (names usable in OVER)
    pub windows: Vec<(String, WindowSpec)>,
    pub items: Vec<SelectItem>,
    pub from_items: Vec<FromItem>,
    pub where_: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
    pub order_by: Vec<(Expr, bool, bool)>, // (key expr, descending, nulls_first)
    pub limit: Option<Expr>,
    pub offset: Option<Expr>,
}

/// ORDER BY keys + LIMIT/OFFSET of a compound statement.
pub type OrderLimitTail = (Vec<(Expr, bool, bool)>, Option<Expr>, Option<Expr>);

#[derive(Clone, Debug)]
pub struct ColumnDef {
    pub name: String,
    pub ty: Ty,
    pub not_null: bool,
    /// PRIMARY KEY: implies not-null + unique (implicit <t>_pkey index)
    pub primary_key: bool,
    /// UNIQUE constraint (implicit <t>_<col>_key index)
    pub unique: bool,
    /// DEFAULT <expr> for INSERTs that omit the column
    pub default: Option<Expr>,
    /// CHECK (<expr>) evaluated on every written row
    pub check: Option<Expr>,
}

#[derive(Clone, Debug)]
pub enum Stmt {
    Select(SelectStmt),
    Union {
        arms: Vec<Stmt>,
        all: bool,                                 // UNION ALL
        order_by: Vec<(Expr, bool, bool)>,         // applied to the combined set
        limit: Option<Expr>,
        offset: Option<Expr>,
    },
    CreateTable {
        name: String,
        if_not_exists: bool,
        cols: Vec<ColumnDef>,
    },
    DropTable {
        name: String,
        if_exists: bool,
    },
    Insert {
        table: TableRef,
        cols: Vec<String>, // explicit column list (empty = all, in order)
        rows: Vec<Vec<Expr>>, // VALUES rows (empty when query is set)
        query: Option<Box<Stmt>>, // INSERT INTO ... SELECT ...
        conflict: Option<OnConflict>, // ON CONFLICT [()] DO ...
        returning: Vec<SelectItem>,
    },
    Update {
        table: TableRef,
        sets: Vec<(String, Expr)>,
        where_: Option<Expr>,
        returning: Vec<SelectItem>,
    },
    Delete {
        table: TableRef,
        where_: Option<Expr>,
        returning: Vec<SelectItem>,
    },
    /// WITH name [(cols)] AS (query) [, ...] attached to a
    /// SELECT / INSERT / UPDATE / DELETE body. Bodies are materialized
    /// once and shared by every reference (PG MATERIALIZED semantics).
    With {
        recursive: bool,
        ctes: Vec<CteDef>,
        body: Box<Stmt>,
    },
    /// VALUES (1,'a'),(2,'b') as a statement or subquery body. Columns
    /// are named column1..columnN (PG). order/limit apply at statement
    /// level only.
    Values {
        rows: Vec<Vec<Expr>>,
        order_by: Vec<(Expr, bool, bool)>,
        limit: Option<Expr>,
        offset: Option<Expr>,
    },
    Begin,
    Commit,
    Rollback,
    Checkpoint,
    CreateIndex {
        /// None = auto-generated `<table>_<col>_idx`
        name: Option<String>,
        if_not_exists: bool,
        table: String,
        method: IdxMethod,
        col: String,
        /// CREATE UNIQUE INDEX (v2.2): the index enforces uniqueness
        unique: bool,
    },
    DropIndex {
        name: String,
        if_exists: bool,
    },
    AlterAddColumn {
        table: String,
        if_exists: bool,
        if_not_exists: bool,
        col: ColumnDef,
    },
    CreateSnapshot {
        name: String,
    },
    DropSnapshot {
        name: String,
        if_exists: bool,
    },
    CopyIn {
        table: TableRef,
        cols: Vec<String>,
        opts: CopyOpts,
    },
    CopyOut {
        src: CopySource,
        opts: CopyOpts,
    },
}

/// Source of a COPY ... TO STDOUT.
#[derive(Clone, Debug)]
pub enum CopySource {
    Table(TableRef, Vec<String>),
    Query(Box<SelectStmt>),
}

/// COPY text/csv options we understand.
#[derive(Clone, Debug)]
pub struct CopyOpts {
    pub csv: bool,
    pub delimiter: char,
    pub null: String,
    pub quote: char,
    pub header: bool,
}

impl Default for CopyOpts {
    fn default() -> Self {
        CopyOpts { csv: false, delimiter: '\t', null: "\\N".into(), quote: '"', header: false }
    }
}

/// Secondary index access method.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IdxMethod {
    BTree,
    Hash,
}

// ------------------------------------------------------------------ parser --

pub struct Parser {
    toks: Vec<Tok>,
    pos: usize,
    depth: usize,
}

/// Maximum expression-nesting depth accepted by the parser. Guard against
/// stack exhaustion on hostile inputs; connection threads run small stacks.
const MAX_EXPR_DEPTH: usize = 128;

const RESERVED_AFTER_EXPR: &[&str] = &[
    "from", "where", "as", "and", "or", "not", "is", "null", "true", "false", "order", "by",
    "limit", "offset", "group", "having", "asc", "desc", "case", "when", "then", "else", "end",
    "like", "in", "between", "on", "join", "inner", "left", "right", "full", "outer", "cross",
    "union", "returning", "set", "values", "into", "distinct", "over", "filter", "window",
];

const RESERVED_AFTER_TABLE: &[&str] = &[
    "on", "join", "inner", "left", "right", "full", "outer", "cross", "where", "group", "order",
    "limit", "having", "union", "returning", "set", "as", "by", "offset", "values",
    "from", "to", "select", "with", "conflict", "window", "over", "filter", "using",
];

impl Parser {
    pub fn new(toks: Vec<Tok>) -> Parser {
        Parser { toks, pos: 0, depth: 0 }
    }

    fn peek(&self) -> &Tok {
        &self.toks[self.pos.min(self.toks.len() - 1)]
    }

    fn peek_at(&self, n: usize) -> &Tok {
        &self.toks[(self.pos + n).min(self.toks.len() - 1)]
    }

    fn next(&mut self) -> Tok {
        let t = self.peek().clone();
        if self.pos < self.toks.len() - 1 {
            self.pos += 1;
        }
        t
    }

    fn at_eof(&self) -> bool {
        matches!(self.peek(), Tok::Eof)
    }

    fn kw(&mut self, k: &str) -> bool {
        if let Tok::Ident(s) = self.peek() {
            if s == k {
                self.next();
                return true;
            }
        }
        false
    }

    fn peek_kw(&self, k: &str) -> bool {
        matches!(self.peek(), Tok::Ident(s) if s == k)
    }

    fn op(&mut self, o: &str) -> bool {
        if let Tok::Op(s) = self.peek() {
            if *s == o {
                self.next();
                return true;
            }
        }
        false
    }

    fn ident(&mut self, what: &str) -> Result<String, String> {
        match self.next() {
            Tok::Ident(s) => Ok(s),
            t => Err(format!("expected {}, found {:?}", what, t)),
        }
    }

    /// Parses the next single statement (None at end). Statements are parsed
    /// and executed one at a time, matching PG simple-query semantics.
    pub fn parse_one(&mut self) -> Result<Option<Stmt>, String> {
        while self.op(";") {}
        if self.at_eof() {
            return Ok(None);
        }
        let s = self.statement()?;
        if !self.at_eof() && !matches!(self.peek(), Tok::Op(";")) {
            return Err(format!("unexpected token {:?}", self.peek()));
        }
        Ok(Some(s))
    }

    /// Parses a whole query string into statements.
    pub fn parse_all(&mut self) -> Result<Vec<Stmt>, String> {
        let mut out = Vec::new();
        while let Some(s) = self.parse_one()? {
            out.push(s);
        }
        Ok(out)
    }

    fn statement(&mut self) -> Result<Stmt, String> {
        if self.kw("with") {
            return self.with_stmt();
        }
        if self.kw("select") {
            let first = self.select_stmt()?;
            if self.peek_kw("union") {
                return self.union_rest(Stmt::Select(first));
            }
            return Ok(Stmt::Select(first));
        }
        if self.kw("values") {
            let rows = self.values_rows()?;
            if self.peek_kw("union") {
                // compound: ORDER BY / LIMIT belong to the compound parser
                let v = Stmt::Values {
                    rows,
                    order_by: Vec::new(),
                    limit: None,
                    offset: None,
                };
                return self.union_rest(v);
            }
            let (order_by, limit, offset) = self.order_limit_tail()?;
            return Ok(Stmt::Values {
                rows,
                order_by,
                limit,
                offset,
            });
        }
        if self.kw("copy") {
            return self.copy_stmt();
        }
        if self.kw("alter") {
            return self.alter_stmt();
        }
        if self.kw("create") {
            if self.kw("table") {
                let if_not_exists = if self.kw("if") {
                    if !self.kw("not") {
                        return Err("expected NOT after IF".into());
                    }
                    self.kw("exists")
                } else {
                    false
                };
                let name = self.table_name()?;
                if !self.op("(") {
                    return Err("expected ( after table name".into());
                }
                let mut cols: Vec<ColumnDef> = Vec::new();
                // table-level constraints attach to columns (single-column
                // forms); parsed inside the loop so they can follow a comma
                macro_rules! table_level {
                    () => {
                        if self.kw("primary") {
                            if !self.kw("key") || !self.op("(") {
                                return Err("expected KEY ( after table-level PRIMARY".into());
                            }
                            let c = self.ident("primary key column")?;
                            if !self.op(")") {
                                return Err("expected ) after PRIMARY KEY column".into());
                            }
                            let cd = cols
                                .iter_mut()
                                .find(|c2| c2.name == c)
                                .ok_or_else(|| format!("PRIMARY KEY column \"{}\" not in table", c))?;
                            cd.primary_key = true;
                            cd.not_null = true;
                            true
                        } else if self.kw("unique") {
                            if !self.op("(") {
                                return Err("expected ( after table-level UNIQUE".into());
                            }
                            let c = self.ident("unique column")?;
                            if !self.op(")") {
                                return Err("expected ) after UNIQUE column".into());
                            }
                            let cd = cols
                                .iter_mut()
                                .find(|c2| c2.name == c)
                                .ok_or_else(|| format!("UNIQUE column \"{}\" not in table", c))?;
                            cd.unique = true;
                            true
                        } else if self.kw("check") {
                            if !self.op("(") {
                                return Err("expected ( after table-level CHECK".into());
                            }
                            let e = self.expr()?;
                            if !self.op(")") {
                                return Err("expected ) after CHECK expression".into());
                            }
                            match cols.iter_mut().find(|c2| c2.check.is_none()) {
                                Some(cd) => cd.check = Some(e),
                                None => {
                                    return Err(
                                        "multiple table-level CHECK constraints not supported"
                                            .into(),
                                    )
                                }
                            }
                            true
                        } else {
                            false
                        }
                    };
                }
                loop {
                    // table-level constraint: PRIMARY KEY (c) / UNIQUE (c) /
                    // CHECK (expr) — only after at least one column
                    if !cols.is_empty() && table_level!() {
                        if self.op(",") {
                            continue;
                        }
                        break;
                    }
                    let cname = self.ident("column name")?;
                    let ty = self.parse_type()?;
                    let mut cd = ColumnDef {
                        name: cname,
                        ty,
                        not_null: false,
                        primary_key: false,
                        unique: false,
                        default: None,
                        check: None,
                    };
                    // column constraints, any order
                    loop {
                        if self.kw("not") {
                            if !self.kw("null") {
                                return Err("expected NULL after NOT".into());
                            }
                            cd.not_null = true;
                        } else if self.kw("null") {
                        } else if self.kw("primary") {
                            if !self.kw("key") {
                                return Err("expected KEY after PRIMARY".into());
                            }
                            cd.primary_key = true;
                            cd.not_null = true;
                        } else if self.kw("unique") {
                            cd.unique = true;
                        } else if self.kw("default") {
                            cd.default = Some(self.expr()?);
                        } else if self.kw("check") {
                            if !self.op("(") {
                                return Err("expected ( after CHECK".into());
                            }
                            cd.check = Some(self.expr()?);
                            if !self.op(")") {
                                return Err("expected ) after CHECK expression".into());
                            }
                        } else {
                            break;
                        }
                    }
                    cols.push(cd);
                    if self.op(",") {
                        continue;
                    }
                    break;
                }
                if !self.op(")") {
                    return Err("expected ) after column list".into());
                }
                return Ok(Stmt::CreateTable {
                    name,
                    if_not_exists,
                    cols,
                });
            }
            let unique = self.kw("unique");
            if self.kw("index") {
                let if_not_exists = if self.kw("if") {
                    if !self.kw("not") {
                        return Err("expected NOT after IF".into());
                    }
                    self.kw("exists")
                } else {
                    false
                };
                // name is optional (PG auto-names); detect it: an ident that
                // is not the ON keyword
                let name = if let Tok::Ident(n) = self.peek() {
                    if n == "on" {
                        None
                    } else {
                        Some(self.ident("index name")?)
                    }
                } else {
                    None
                };
                if !self.kw("on") {
                    return Err("expected ON after CREATE INDEX".into());
                }
                let table = self.ident("table name")?;
                let mut method = IdxMethod::BTree;
                if self.kw("using") {
                    if self.kw("btree") {
                        method = IdxMethod::BTree;
                    } else if self.kw("hash") {
                        method = IdxMethod::Hash;
                    } else {
                        return Err("expected btree or hash after USING".into());
                    }
                }
                if !self.op("(") {
                    return Err("expected ( column ) after index table".into());
                }
                let col = self.ident("column name")?;
                // optional ASC/DESC (accepted, order is irrelevant: rowids)
                let _ = self.kw("asc") || self.kw("desc");
                if !self.op(")") {
                    return Err("expected ) after index column".into());
                }
                return Ok(Stmt::CreateIndex {
                    name,
                    if_not_exists,
                    table,
                    method,
                    col,
                    unique,
                });
            }
            if self.kw("snapshot") {
                let name = match self.next() {
                    Tok::Ident(v) => v,
                    Tok::Str(v) => v,
                    t => return Err(format!("expected snapshot name, found {:?}", t)),
                };
                return Ok(Stmt::CreateSnapshot { name });
            }
            return Err("expected TABLE, INDEX or SNAPSHOT after CREATE".into());
        }
        if self.kw("drop") {
            if self.kw("index") {
                let if_exists = if self.kw("if") {
                    if !self.kw("exists") {
                        return Err("expected EXISTS after IF".into());
                    }
                    true
                } else {
                    false
                };
                let name = self.ident("index name")?;
                return Ok(Stmt::DropIndex { name, if_exists });
            }
            if self.kw("table") {
                let if_exists = if self.kw("if") {
                    if !self.kw("exists") {
                        return Err("expected EXISTS after IF".into());
                    }
                    true
                } else {
                    false
                };
                let name = self.table_name()?;
                return Ok(Stmt::DropTable { name, if_exists });
            }
            if self.kw("snapshot") {
                let if_exists = if self.kw("if") {
                    if !self.kw("exists") {
                        return Err("expected EXISTS after IF".into());
                    }
                    true
                } else {
                    false
                };
                let name = match self.next() {
                    Tok::Ident(v) => v,
                    Tok::Str(v) => v,
                    t => return Err(format!("expected snapshot name, found {:?}", t)),
                };
                return Ok(Stmt::DropSnapshot { name, if_exists });
            }
            return Err("expected TABLE, INDEX or SNAPSHOT after DROP".into());
        }
        if self.kw("insert") {
            if !self.kw("into") {
                return Err("expected INTO after INSERT".into());
            }
            let table = self.table_ref(false)?;
            let mut cols: Vec<String> = Vec::new();
            if self.op("(") {
                loop {
                    cols.push(self.ident("column name")?);
                    if self.op(",") {
                        continue;
                    }
                    break;
                }
                if !self.op(")") {
                    return Err("expected ) after column list".into());
                }
            }
            // source: VALUES rows or a full query (SELECT / WITH / parens)
            let mut rows: Vec<Vec<Expr>> = Vec::new();
            let mut query: Option<Box<Stmt>> = None;
            if self.kw("values") {
                rows = self.values_rows()?;
            } else if self.peek_kw("select")
                || self.peek_kw("with")
                || matches!(self.peek(), Tok::Op("("))
            {
                let q = self.query_body()?;
                query = Some(Box::new(q));
            } else {
                return Err("expected VALUES or SELECT in INSERT".into());
            }
            // ON CONFLICT [(col)] DO NOTHING | DO UPDATE SET ...
            let mut conflict: Option<OnConflict> = None;
            if self.kw("on") {
                if !self.kw("conflict") {
                    return Err("expected CONFLICT after ON".into());
                }
                let mut target = Vec::new();
                if self.op("(") {
                    loop {
                        target.push(self.ident("conflict target column")?);
                        if self.op(",") {
                            continue;
                        }
                        if self.op(")") {
                            break;
                        }
                        return Err("expected , or ) after conflict target".into());
                    }
                }
                if !self.kw("do") {
                    return Err("expected DO after ON CONFLICT target".into());
                }
                let action = if self.kw("nothing") {
                    ConflictAction::Nothing
                } else if self.kw("update") {
                    if !self.kw("set") {
                        return Err("expected SET after DO UPDATE".into());
                    }
                    let mut sets = Vec::new();
                    loop {
                        let col = self.ident("column name")?;
                        if !self.op("=") {
                            return Err("expected = in SET".into());
                        }
                        let e = self.expr()?;
                        sets.push((col, e));
                        if self.op(",") {
                            continue;
                        }
                        break;
                    }
                    let guard = if self.kw("where") {
                        Some(self.expr()?)
                    } else {
                        None
                    };
                    ConflictAction::Update { sets, guard }
                } else {
                    return Err("expected NOTHING or UPDATE after DO".into());
                };
                conflict = Some(OnConflict { target, action });
            }
            let returning = self.parse_returning()?;
            return Ok(Stmt::Insert {
                table,
                cols,
                rows,
                query,
                conflict,
                returning,
            });
        }
        if self.kw("update") {
            let table = self.table_ref(false)?;
            if !self.kw("set") {
                return Err("expected SET after table name".into());
            }
            let mut sets = Vec::new();
            loop {
                let col = self.ident("column name")?;
                if !self.op("=") {
                    return Err("expected = in SET".into());
                }
                let e = self.expr()?;
                sets.push((col, e));
                if self.op(",") {
                    continue;
                }
                break;
            }
            let where_ = if self.kw("where") {
                Some(self.expr()?)
            } else {
                None
            };
            let returning = self.parse_returning()?;
            return Ok(Stmt::Update {
                table,
                sets,
                where_,
                returning,
            });
        }
        if self.kw("delete") {
            if !self.kw("from") {
                return Err("expected FROM after DELETE".into());
            }
            let table = self.table_ref(false)?;
            let where_ = if self.kw("where") {
                Some(self.expr()?)
            } else {
                None
            };
            let returning = self.parse_returning()?;
            return Ok(Stmt::Delete {
                table,
                where_,
                returning,
            });
        }
        if self.kw("checkpoint") {
            return Ok(Stmt::Checkpoint);
        }
        if self.kw("begin") {
            self.kw("transaction");
            self.kw("work");
            return Ok(Stmt::Begin);
        }
        if self.kw("commit") {
            self.kw("transaction");
            self.kw("work");
            return Ok(Stmt::Commit);
        }
        if self.kw("rollback") {
            self.kw("transaction");
            self.kw("work");
            return Ok(Stmt::Rollback);
        }
        Err(format!("unexpected token {:?}", self.peek()))
    }

    fn parse_returning(&mut self) -> Result<Vec<SelectItem>, String> {
        if self.kw("returning") {
            let mut items = Vec::new();
            loop {
                if self.op("*") {
                    items.push(SelectItem::Star);
                } else {
                    let e = self.expr()?;
                    let alias = if self.kw("as") {
                        Some(self.ident("alias")?)
                    } else {
                        None
                    };
                    items.push(SelectItem::Expr(e, alias));
                }
                if self.op(",") {
                    continue;
                }
                break;
            }
            Ok(items)
        } else {
            Ok(Vec::new())
        }
    }

    fn parse_type(&mut self) -> Result<Ty, String> {
        let mut t = self.ident("type name")?;
        // schema-qualified type (pg_catalog.text): use the bare name
        if self.op(".") {
            t = self.ident("type name")?;
        }
        // array suffix (int2[]): arrays are not represented; the element
        // type is kept so catalog probes parse
        while self.op("[") {
            if !self.op("]") {
                return Err("expected ] in array type".into());
            }
        }
        // tolerate multiword types like "double precision"
        if t == "double" && self.peek_kw("precision") {
            self.next();
            t = "float8".into();
        }
        match t.as_str() {
            "bool" | "boolean" => Ok(Ty::Bool),
            "int" | "integer" | "int4" | "bigint" | "int8" | "smallint" | "int2" => Ok(Ty::Int),
            "real" | "float" | "float4" | "float8" | "double" | "numeric" | "decimal" => {
                Ok(Ty::Float)
            }
            "text" | "varchar" | "char" | "string" | "bpchar" | "name" | "regtype"
            | "regclass" | "regnamespace" | "oid" => Ok(Ty::Text),
            "date" => Ok(Ty::Date),
            "time" => {
                // "time [without time zone]" (and timetz -> plain time)
                if (self.kw("without") || self.kw("with"))
                    && !(self.kw("time") && self.kw("zone"))
                {
                    return Err("expected TIME ZONE here".into());
                }
                Ok(Ty::Time)
            }
            "timestamp" => {
                // "timestamp [without time zone]"; with-zone stays naive
                if (self.kw("without") || self.kw("with"))
                    && !(self.kw("time") && self.kw("zone"))
                {
                    return Err("expected TIME ZONE here".into());
                }
                Ok(Ty::Timestamp)
            }
            "timestamptz" => Ok(Ty::Timestamp),
            "timetz" => Ok(Ty::Time),
            "interval" => Ok(Ty::Interval),
            _ => Err(format!("unknown type '{}'", t)),
        }
    }

    /// Table name in DDL/DML positions: accepts an optional schema prefix.
    /// "public.x" folds to "x"; other schemas stay fully qualified (they
    /// will not match user tables and fail with a clean not-found).
    fn table_name(&mut self) -> Result<String, String> {
        let mut name = self.ident("table name")?;
        if self.op(".") {
            let second = self.ident("table name")?;
            if name != "public" {
                name = format!("{}.{}", name, second);
            } else {
                name = second;
            }
        }
        Ok(name)
    }

    fn alter_stmt(&mut self) -> Result<Stmt, String> {
        if !self.kw("table") {
            return Err("only ALTER TABLE ... ADD COLUMN is supported".into());
        }
        let if_exists = if self.kw("if") {
            if !self.kw("exists") {
                return Err("expected EXISTS after IF".into());
            }
            true
        } else {
            false
        };
        let table = self.table_name()?;
        if !self.kw("add") {
            return Err("only ALTER TABLE ... ADD COLUMN is supported".into());
        }
        self.kw("column"); // optional
        let if_not_exists = if self.kw("if") {
            if !self.kw("not") {
                return Err("expected NOT after IF".into());
            }
            self.kw("exists")
        } else {
            false
        };
        let name = self.ident("column name")?;
        let ty = self.parse_type()?;
        let mut not_null = false;
        loop {
            if self.kw("not") {
                if !self.kw("null") {
                    return Err("expected NULL after NOT".into());
                }
                not_null = true;
            } else if self.kw("null") {
            } else if self.kw("default") {
                return Err("ALTER TABLE ... ADD COLUMN with DEFAULT is not supported".into());
            } else {
                break;
            }
        }
        if not_null {
            return Err(
                "ALTER TABLE ... ADD COLUMN: only nullable columns (no NOT NULL/DEFAULT)".into(),
            );
        }
        Ok(Stmt::AlterAddColumn {
            table,
            if_exists,
            if_not_exists,
            col: ColumnDef {
                name,
                ty,
                not_null,
                primary_key: false,
                unique: false,
                default: None,
                check: None,
            },
        })
    }

    fn copy_stmt(&mut self) -> Result<Stmt, String> {
        let mut opts = CopyOpts::default();
        if self.op("(") {
            // COPY (SELECT ...) TO STDOUT
            if !self.kw("select") {
                return Err("expected SELECT in COPY (query)".into());
            }
            let sel = self.select_stmt()?;
            if !self.op(")") {
                return Err("expected ) after COPY query".into());
            }
            if !self.kw("to") {
                return Err("expected TO after COPY (query)".into());
            }
            let dir = self.ident("STDIN or STDOUT")?;
            if dir != "stdout" {
                return Err("COPY (query) only supports TO STDOUT".into());
            }
            self.copy_opts(&mut opts)?;
            return Ok(Stmt::CopyOut { src: CopySource::Query(Box::new(sel)), opts });
        }
        let table = self.table_ref(false)?;
        let mut cols = Vec::new();
        if self.op("(") {
            loop {
                cols.push(self.ident("column name")?);
                if self.op(",") { continue; }
                if self.op(")") { break; }
                return Err("expected , or ) in COPY column list".into());
            }
        }
        let from = if self.kw("from") { true } else if self.kw("to") { false } else {
            return Err("expected FROM or TO in COPY".into());
        };
        let dir = self.ident("STDIN or STDOUT")?;
        match (from, dir.as_str()) {
            (true, "stdin") => {}
            (false, "stdout") => {}
            _ => return Err("COPY only supports FROM STDIN and TO STDOUT".into()),
        }
        self.copy_opts(&mut opts)?;
        if from {
            Ok(Stmt::CopyIn { table, cols, opts })
        } else {
            Ok(Stmt::CopyOut { src: CopySource::Table(table, cols), opts })
        }
    }

    fn str_lit(&mut self, what: &str) -> Result<String, String> {
        match self.next() {
            Tok::Str(v) => Ok(v),
            t => Err(format!("expected string literal for {} (got {:?})", what, t)),
        }
    }

    /// Parses optional WITH (...) / DELIMITER / NULL / CSV / HEADER suffix.
    fn copy_opts(&mut self, opts: &mut CopyOpts) -> Result<(), String> {
        let mut have_with = false;
        if self.kw("with") {
            have_with = true;
            if !self.op("(") {
                // WITH CSV / WITH HEADER without parens
                have_with = false;
            }
        }
        if have_with {
            loop {
                let key = self.ident("COPY option")?;
                match key.as_str() {
                    "format" => {
                        let _ = self.op("=");
                        let v = self.ident("FORMAT value")?;
                        match v.as_str() {
                            "text" => opts.csv = false,
                            "csv" => opts.csv = true,
                            other => return Err(format!("unsupported COPY FORMAT {} (text|csv)", other)),
                        }
                    }
                    "delimiter" => {
                        let _ = self.op("=");
                        let v = self.str_lit("DELIMITER value")?;
                        let mut it = v.chars();
                        let d = it.next().ok_or("empty DELIMITER")?;
                        if it.next().is_some() { return Err("DELIMITER must be one character".into()); }
                        opts.delimiter = d;
                    }
                    "null" => {
                        let _ = self.op("=");
                        opts.null = self.str_lit("NULL value")?;
                    }
                    "quote" => {
                        let _ = self.op("=");
                        let v = self.str_lit("QUOTE value")?;
                        let mut it = v.chars();
                        let q = it.next().ok_or("empty QUOTE")?;
                        if it.next().is_some() { return Err("QUOTE must be one character".into()); }
                        opts.quote = q;
                    }
                    "header" => {
                        // HEADER [ true | false | match ] — bare or = value
                        let v = if self.op("=") {
                            Some(self.ident("HEADER value")?)
                        } else if let Tok::Ident(id) = self.peek() {
                            let b = matches!(id.as_str(), "true" | "false" | "match");
                            if b {
                                let v = id.clone();
                                self.next();
                                Some(v)
                            } else {
                                None
                            }
                        } else {
                            None
                        };
                        opts.header = v.is_none_or(|x| x != "false");
                    }
                    other => return Err(format!("unsupported COPY option {} ", other)),
                }
                if self.op(",") { continue; }
                if self.op(")") { break; }
                return Err("expected , or ) in COPY options".into());
            }
            if opts.csv {
                if opts.delimiter == '\t' { opts.delimiter = ','; }
                if opts.null == "\\N" { opts.null = String::new(); }
            }
            return Ok(());
        }
        // bare suffixes: CSV, HEADER, DELIMITER 'x'
        loop {
            if self.kw("csv") {
                opts.csv = true;
                if opts.delimiter == '\t' {
                    opts.delimiter = ',';
                }
                if opts.null == "\\N" {
                    opts.null = String::new();
                }
                continue;
            }
            if self.kw("header") {
                opts.header = true;
                continue;
            }
            if self.kw("delimiter") {
                let v = self.str_lit("DELIMITER value")?;
                let mut it = v.chars();
                let d = it.next().ok_or("empty DELIMITER")?;
                if it.next().is_some() {
                    return Err("DELIMITER must be one character".into());
                }
                opts.delimiter = d;
                continue;
            }
            break;
        }
        Ok(())
    }

    fn table_ref(&mut self, from_position: bool) -> Result<TableRef, String> {
        // derived table: FROM (SELECT ...) [AS] x [(a, b)] — only legal in
        // FROM position (a paren after a DML/COPY table name is a column
        // list, never a subquery)
        if from_position && matches!(self.peek(), Tok::Op("(")) {
            self.next(); // (
            let sq = self.subquery_body()?;
            if !self.op(")") {
                return Err("expected ) after derived table subquery".into());
            }
            // alias: AS x / bare x, then an optional column alias list
            let alias = if self.peek_kw("as") {
                self.next();
                Some(self.ident("subquery alias")?)
            } else if matches!(
                self.peek(),
                Tok::Ident(ref s) if !RESERVED_AFTER_TABLE.contains(&s.as_str())
            ) {
                match self.next() {
                    Tok::Ident(v) => Some(v),
                    _ => None,
                }
            } else {
                None
            };
            let mut col_aliases = Vec::new();
            if self.op("(") {
                loop {
                    col_aliases.push(self.ident("column alias")?);
                    if self.op(",") {
                        continue;
                    }
                    if self.op(")") {
                        break;
                    }
                    return Err("expected , or ) in column alias list".into());
                }
            }
            let name = alias.clone().unwrap_or_default();
            return Ok(TableRef {
                name,
                alias,
                as_of: None,
                func_args: None,
                col_aliases,
                subquery: Some(Box::new(sq)),
            });
        }
        let mut name = self.ident("table name")?;
        if self.op(".") {
            // schema-qualified: keep the full name for catalog routing,
            // but fold public. to the bare user-table name
            let second = self.ident("table name")?;
            name = if name == "public" {
                second
            } else {
                format!("{}.{}", name, second)
            };
        }
        // set-returning function: FROM generate_series(a, b) AS s
        // (only in FROM position — a paren after a DML/COPY table name is
        // always a column list)
        let func_args = if from_position && matches!(self.peek(), Tok::Op("(")) {
            self.next(); // (
            let mut args = Vec::new();
            if !self.op(")") {
                loop {
                    args.push(self.expr()?);
                    if self.op(",") {
                        continue;
                    }
                    break;
                }
                if !self.op(")") {
                    return Err("expected ) after table function arguments".into());
                }
            }
            Some(args)
        } else {
            None
        };
        // AS OF time travel (FROM position only): t AS OF SNAPSHOT s /
        // t AS OF LSN 42. Parsed before the alias branch so the AS isn't
        // taken for one.
        let mut as_of = None;
        if from_position
            && self.peek_kw("as")
            && matches!(self.peek_at(1), Tok::Ident(w) if w == "of")
        {
            self.next(); // AS
            self.next(); // OF
            if self.kw("snapshot") {
                let name = match self.next() {
                    Tok::Ident(v) => v,
                    Tok::Str(v) => v,
                    t => return Err(format!("expected snapshot name, found {:?}", t)),
                };
                as_of = Some(AsOf::Snapshot(name));
            } else if self.kw("lsn") {
                let n = match self.next() {
                    Tok::Num(v) => v
                        .parse::<u64>()
                        .map_err(|_| format!("bad LSN number {:?}", v))?,
                    t => return Err(format!("expected LSN number, found {:?}", t)),
                };
                as_of = Some(AsOf::Lsn(n));
            } else {
                return Err("expected SNAPSHOT or LSN after AS OF".into());
            }
        }
        let alias = if self.kw("as") {
            let a = self.ident("alias")?;
            if !from_position
                && a == "of"
                && (self.peek_kw("snapshot") || self.peek_kw("lsn"))
            {
                return Err("AS OF is only allowed in a FROM clause".into());
            }
            Some(a)
        } else if let Tok::Ident(s) = self.peek() {
            if !RESERVED_AFTER_TABLE.contains(&s.as_str()) {
                let a = s.clone();
                self.next();
                Some(a)
            } else {
                None
            }
        } else {
            None
        };
        // column alias list: FROM t AS x (a, b) / FROM g(1,2) s(g)
        // (FROM position only — after a DML/COPY table name a paren is
        // always that statement's own column list)
        let mut col_aliases = Vec::new();
        if from_position && self.op("(") {
            loop {
                col_aliases.push(self.ident("column alias")?);
                if self.op(",") {
                    continue;
                }
                if self.op(")") {
                    break;
                }
                return Err("expected , or ) in column alias list".into());
            }
        }
        Ok(TableRef {
            name,
            alias,
            as_of,
            func_args,
            col_aliases,
            subquery: None,
        })
    }

    /// WITH name [(cols)] AS [NOT] MATERIALIZED (query) [, ...] <stmt>
    /// FILTER (WHERE expr) after an aggregate call's closing paren.
    fn filter_clause(&mut self) -> Result<Option<Expr>, String> {
        if !self.kw("filter") {
            return Ok(None);
        }
        if !self.op("(") {
            return Err("expected ( after FILTER".into());
        }
        if !self.kw("where") {
            return Err("expected WHERE in FILTER clause".into());
        }
        let e = self.expr()?;
        if !self.op(")") {
            return Err("expected ) after FILTER clause".into());
        }
        Ok(Some(e))
    }

    /// (expr, ...), (expr, ...) rows after a VALUES keyword.
    fn values_rows(&mut self) -> Result<Vec<Vec<Expr>>, String> {
        let mut rows = Vec::new();
        loop {
            if !self.op("(") {
                return Err("expected ( to start VALUES row".into());
            }
            let mut row = Vec::new();
            loop {
                row.push(self.expr()?);
                if self.op(",") {
                    continue;
                }
                break;
            }
            if !self.op(")") {
                return Err("expected ) to end VALUES row".into());
            }
            rows.push(row);
            if self.op(",") {
                continue;
            }
            break;
        }
        Ok(rows)
    }

    /// Trailing ORDER BY / LIMIT / OFFSET of a compound statement
    /// (shared by UNION and VALUES).
    fn order_limit_tail(&mut self) -> Result<OrderLimitTail, String> {
        let mut order_by = Vec::new();
        while self.kw("order") {
            if !self.kw("by") {
                return Err("expected BY after ORDER".into());
            }
            loop {
                let e = self.expr()?;
                let mut desc = false;
                let mut nulls_last = false;
                if self.kw("asc") {
                } else if self.kw("desc") {
                    desc = true;
                }
                if self.kw("nulls") {
                    if self.kw("first") {
                        nulls_last = false;
                    } else if self.kw("last") {
                        nulls_last = true;
                    } else {
                        return Err("expected FIRST or LAST after NULLS".into());
                    }
                }
                order_by.push((e, desc, nulls_last));
                if self.op(",") {
                    continue;
                }
                break;
            }
        }
        let limit = if self.kw("limit") {
            Some(self.expr()?)
        } else {
            None
        };
        let offset = if self.kw("offset") {
            Some(self.expr()?)
        } else {
            None
        };
        Ok((order_by, limit, offset))
    }

    fn with_stmt(&mut self) -> Result<Stmt, String> {
        let recursive = self.kw("recursive");
        let mut ctes = Vec::new();
        loop {
            let name = self.ident("CTE name")?;
            let mut col_aliases = Vec::new();
            if self.op("(") {
                loop {
                    col_aliases.push(self.ident("column alias")?);
                    if self.op(",") {
                        continue;
                    }
                    if self.op(")") {
                        break;
                    }
                    return Err("expected , or ) in CTE column alias list".into());
                }
            }
            if !self.kw("as") {
                return Err("expected AS after CTE name".into());
            }
            // MATERIALIZED / NOT MATERIALIZED: accepted; puddle always
            // materializes once (PG MATERIALIZED semantics)
            let _ = self.kw("not");
            let _ = self.kw("materialized");
            if !self.op("(") {
                return Err("expected ( to start CTE body".into());
            }
            let body = self.query_body()?;
            if !self.op(")") {
                return Err("expected ) after CTE body".into());
            }
            ctes.push(CteDef {
                name,
                col_aliases,
                body: Box::new(body),
            });
            if self.op(",") {
                continue;
            }
            break;
        }
        let body = self.statement()?;
        match body {
            Stmt::Select(_)
            | Stmt::Union { .. }
            | Stmt::Insert { .. }
            | Stmt::Update { .. }
            | Stmt::Delete { .. } => Ok(Stmt::With {
                recursive,
                ctes,
                body: Box::new(body),
            }),
            _ => Err("WITH clause applies to SELECT, INSERT, UPDATE or DELETE".into()),
        }
    }

    /// A query inside parens: SELECT/UNION chain, WITH query, or another
    /// paren layer.
    fn query_body(&mut self) -> Result<Stmt, String> {
        if self.op("(") {
            let inner = self.query_body()?;
            if !self.op(")") {
                return Err("expected ) after subquery".into());
            }
            return Ok(inner);
        }
        if self.kw("with") {
            return self.with_stmt();
        }
        if self.kw("values") {
            let rows = self.values_rows()?;
            let v = Stmt::Values {
                rows,
                order_by: Vec::new(),
                limit: None,
                offset: None,
            };
            if self.peek_kw("union") {
                return self.union_rest(v);
            }
            return Ok(v);
        }
        if !self.kw("select") {
            return Err("expected SELECT in subquery".into());
        }
        let first = self.select_stmt()?;
        if self.peek_kw("union") {
            return self.union_rest(Stmt::Select(first));
        }
        Ok(Stmt::Select(first))
    }

    /// Body of a derived table / CTE: a full SELECT, UNION chain, or
    /// another parenthesized query. The caller consumed the opening ( and
    /// consumes the matching ).
    fn subquery_body(&mut self) -> Result<Stmt, String> {
        if self.op("(") {
            let inner = self.subquery_body()?;
            if !self.op(")") {
                return Err("expected ) after subquery".into());
            }
            return Ok(inner);
        }
        if self.kw("with") {
            return self.with_stmt();
        }
        if self.kw("values") {
            let rows = self.values_rows()?;
            let v = Stmt::Values {
                rows,
                order_by: Vec::new(),
                limit: None,
                offset: None,
            };
            if self.peek_kw("union") {
                return self.union_rest(v);
            }
            return Ok(v);
        }
        if !self.kw("select") {
            return Err("expected SELECT in subquery".into());
        }
        let first = self.select_stmt()?;
        if self.peek_kw("union") {
            return self.union_rest(Stmt::Select(first));
        }
        Ok(Stmt::Select(first))
    }

    /// UNION [ALL] chain after the first arm (ORDER BY/LIMIT/OFFSET belong
    /// to the whole compound, per SQL).
    fn union_rest(&mut self, first: Stmt) -> Result<Stmt, String> {
        let mut arms = vec![first];
        let mut all = false;
        while self.kw("union") {
            if self.kw("all") {
                all = true; // any ALL in the chain keeps duplicates for that arm
            }
            if self.kw("values") {
                // VALUES arm: rows belong to the compound, no own ORDER BY
                arms.push(Stmt::Values {
                    rows: self.values_rows()?,
                    order_by: Vec::new(),
                    limit: None,
                    offset: None,
                });
                continue;
            }
            if !self.kw("select") {
                return Err("expected SELECT after UNION".into());
            }
            arms.push(Stmt::Select(self.select_stmt()?));
        }
        let mut order_by = Vec::new();
        while self.kw("order") {
            if !self.kw("by") {
                return Err("expected BY after ORDER".into());
            }
            loop {
                let e = self.expr()?;
                let desc = if self.kw("desc") {
                    true
                } else {
                    self.kw("asc");
                    false
                };
                let nulls_first = if self.kw("nulls") {
                    if self.kw("first") {
                        true
                    } else if self.kw("last") {
                        false
                    } else {
                        return Err("expected FIRST or LAST after NULLS".into());
                    }
                } else {
                    desc
                };
                order_by.push((e, desc, nulls_first));
                if self.op(",") {
                    continue;
                }
                break;
            }
        }
        let limit = if self.kw("limit") {
            Some(self.expr()?)
        } else {
            None
        };
        let offset = if self.kw("offset") {
            let e = self.expr()?;
            if !self.kw("rows") && !self.kw("row") {
                // "OFFSET n ROWS" / "OFFSET n" both accepted
            }
            Some(e)
        } else {
            None
        };
        // PG grammar: a trailing ORDER BY / LIMIT / OFFSET belongs to the
        // compound, but select_stmt already attached it to the final arm
        // (the arm parser cannot know a UNION follows). Lift it up.
        let (mut order_by, mut limit, mut offset) = (order_by, limit, offset);
        if let Some(Stmt::Select(last)) = arms.last_mut() {
            if !last.order_by.is_empty() {
                let lifted = std::mem::take(&mut last.order_by);
                let extra = std::mem::take(&mut order_by);
                order_by = lifted;
                order_by.extend(extra);
            }
            if limit.is_none() {
                limit = last.limit.take();
            }
            if offset.is_none() {
                offset = last.offset.take();
            }
        }
        Ok(Stmt::Union {
            arms,
            all,
            order_by,
            limit,
            offset,
        })
    }

    /// Parses OVER ( ... ) after a function call.
    fn over_clause(&mut self) -> Result<WindowSpec, String> {
        // an identifier here is a window NAME unless it starts a spec body
        let is_name = |n: &str| {
            !RESERVED_AFTER_EXPR.contains(&n)
                && !matches!(n, "partition" | "order" | "rows" | "range")
        };
        // OVER w  — bare reference to a WINDOW-clause name
        if let Tok::Ident(ref n) = self.peek() {
            if is_name(n) {
                let name = match self.next() {
                    Tok::Ident(v) => v,
                    _ => return Err("expected window name after OVER".into()),
                };
                return Ok(WindowSpec {
                    name: Some(name),
                    partition_by: Vec::new(),
                    order_by: Vec::new(),
                    frame: None,
                });
            }
        }
        if !self.op("(") {
            return Err("expected ( or a window name after OVER".into());
        }
        // OVER ( [w] [ORDER BY ...] [frame] ) — w = base window name to
        // inherit from (PG: PARTITION BY cannot be re-specified)
        if let Tok::Ident(ref n) = self.peek() {
            if is_name(n) {
                let name = match self.next() {
                    Tok::Ident(v) => v,
                    _ => return Err("expected window name".into()),
                };
                // a following PARTITION BY is parsed into the spec; the
                // resolver rejects it with PG's 42P10
                let mut spec = self.window_spec_body()?;
                if !self.op(")") {
                    return Err("expected ) after window specification".into());
                }
                spec.name = Some(name);
                return Ok(spec);
            }
        }
        let spec = self.window_spec_body()?;
        if !self.op(")") {
            return Err("expected ) after window specification".into());
        }
        Ok(spec)
    }

    /// The inside of a window specification: [PARTITION BY ...]
    /// [ORDER BY ...] [frame]. Caller handles the parentheses.
    fn window_spec_body(&mut self) -> Result<WindowSpec, String> {
        let mut spec = WindowSpec {
            name: None,
            partition_by: Vec::new(),
            order_by: Vec::new(),
            frame: None,
        };
        if self.kw("partition") {
            if !self.kw("by") {
                return Err("expected BY after PARTITION".into());
            }
            loop {
                spec.partition_by.push(self.expr()?);
                if self.op(",") {
                    continue;
                }
                break;
            }
        }
        if self.kw("order") {
            if !self.kw("by") {
                return Err("expected BY after ORDER".into());
            }
            loop {
                let e = self.expr()?;
                let desc = if self.kw("desc") {
                    true
                } else {
                    self.kw("asc");
                    false
                };
                let nulls_first = if self.kw("nulls") {
                    if self.kw("first") {
                        true
                    } else if self.kw("last") {
                        false
                    } else {
                        return Err("expected FIRST or LAST after NULLS".into());
                    }
                } else {
                    desc
                };
                spec.order_by.push((e, desc, nulls_first));
                if self.op(",") {
                    continue;
                }
                break;
            }
        }
        // frame: [ROWS|RANGE] BETWEEN bound AND bound, or the single-bound
        // form [ROWS|RANGE] bound (end = CURRENT ROW). RANGE is only
        // supported with UNBOUNDED/CURRENT bounds (PG's default frame).
        let frame_mode = if self.peek_kw("rows") {
            self.next();
            Some(true)
        } else if self.peek_kw("range") {
            self.next();
            Some(false)
        } else {
            None
        };
        if let Some(rows_mode) = frame_mode {
            let between = self.kw("between");
            let start = self.frame_bound(rows_mode)?;
            let end = if between {
                if !self.kw("and") {
                    return Err("expected AND in frame clause".into());
                }
                self.frame_bound(rows_mode)?
            } else {
                FrameBound::CurrentRow
            };
            if matches!(start, FrameBound::UnboundedFollowing) {
                return Err("frame start cannot be UNBOUNDED FOLLOWING".into());
            }
            if matches!(end, FrameBound::UnboundedPreceding) {
                return Err("frame end cannot be UNBOUNDED PRECEDING".into());
            }
            spec.frame = Some(Frame {
                rows: rows_mode,
                start,
                end,
            });
        }
        Ok(spec)
    }

    fn frame_bound(&mut self, rows_mode: bool) -> Result<FrameBound, String> {
        if self.kw("unbounded") {
            if self.kw("preceding") {
                return Ok(FrameBound::UnboundedPreceding);
            }
            if self.kw("following") {
                return Ok(FrameBound::UnboundedFollowing);
            }
            return Err("expected PRECEDING or FOLLOWING after UNBOUNDED".into());
        }
        if self.kw("current") {
            if self.kw("row") {
                return Ok(FrameBound::CurrentRow);
            }
            return Err("expected ROW after CURRENT".into());
        }
        let n = match self.next() {
            Tok::Num(v) => v.parse::<i64>().map_err(|_| {
                "frame offset must be an integer literal".to_string()
            })?,
            t => {
                return Err(format!(
                    "expected a frame offset (integer literal), found {:?}",
                    t
                ))
            }
        };
        if self.kw("preceding") {
            if !rows_mode {
                return Err("RANGE with a numeric offset is not supported".into());
            }
            return Ok(FrameBound::Preceding(n));
        }
        if self.kw("following") {
            if !rows_mode {
                return Err("RANGE with a numeric offset is not supported".into());
            }
            return Ok(FrameBound::Following(n));
        }
        Err("expected PRECEDING or FOLLOWING after frame offset".into())
    }

    fn select_stmt(&mut self) -> Result<SelectStmt, String> {
        let mut windows: Vec<(String, WindowSpec)> = Vec::new();
        let distinct = if self.kw("distinct") {
            true
        } else {
            self.kw("all");
            false
        };
        let mut items = Vec::new();
        loop {
            if self.op("*") {
                items.push(SelectItem::Star);
            } else {
                let e = self.expr()?;
                let alias = if self.kw("as") {
                    Some(self.ident("alias")?)
                } else if let Tok::Ident(s) = self.peek() {
                    if !RESERVED_AFTER_EXPR.contains(&s.as_str()) {
                        let a = s.clone();
                        self.next();
                        Some(a)
                    } else {
                        None
                    }
                } else {
                    None
                };
                items.push(SelectItem::Expr(e, alias));
            }
            if self.op(",") {
                continue;
            }
            break;
        }

        let mut from_items = Vec::new();
        if self.kw("from") {
            loop {
                let base = self.table_ref(true)?;
                let mut joins = Vec::new();
                loop {
                    let kind = if self.kw("join") {
                        JoinKind::Inner
                    } else if self.kw("inner") {
                        if !self.kw("join") {
                            return Err("expected JOIN after INNER".into());
                        }
                        JoinKind::Inner
                    } else if self.kw("left") {
                        self.kw("outer");
                        if !self.kw("join") {
                            return Err("expected JOIN after LEFT".into());
                        }
                        JoinKind::Left
                    } else if self.kw("cross") {
                        if !self.kw("join") {
                            return Err("expected JOIN after CROSS".into());
                        }
                        JoinKind::Cross
                    } else if self.kw("right") {
                        self.kw("outer");
                        if !self.kw("join") {
                            return Err("expected JOIN after RIGHT".into());
                        }
                        JoinKind::Right
                    } else if self.kw("full") {
                        self.kw("outer");
                        if !self.kw("join") {
                            return Err("expected JOIN after FULL".into());
                        }
                        JoinKind::Full
                    } else {
                        break;
                    };
                    let table = self.table_ref(true)?;
                    let on = if kind == JoinKind::Cross {
                        None
                    } else if self.kw("on") {
                        Some(self.expr()?)
                    } else if self.kw("using") {
                        // JOIN ... USING (c1, c2) rewrites to the
                        // equality conjunction on the shared column
                        // names. (PG additionally merges the output
                        // columns; puddle keeps both, so the bare name
                        // stays resolvable only when unambiguous.)
                        if !self.op("(") {
                            return Err("expected ( after USING".into());
                        }
                        let mut cols: Vec<String> = Vec::new();
                        loop {
                            cols.push(self.ident("column name")?);
                            if self.op(",") {
                                continue;
                            }
                            break;
                        }
                        if !self.op(")") {
                            return Err("expected ) after USING list".into());
                        }
                        if cols.is_empty() {
                            return Err("USING list cannot be empty".into());
                        }
                        // both sides qualified: the right with its table
                        // name, the left with the FROM item's base table.
                        // (PG merges the two output columns; puddle keeps
                        // both, so a bare `id` in the SELECT list stays
                        // ambiguous — qualify it. And in a chained join
                        // the shared column must live in the base table.)
                        let lhs = base.effective_name().to_string();
                        let rhs = table.effective_name().to_string();
                        let mut cond: Option<Expr> = None;
                        for cn in cols {
                            let eq = Expr::Bin(
                                Box::new(Expr::QCol(lhs.clone(), cn.clone())),
                                "=",
                                Box::new(Expr::QCol(rhs.clone(), cn)),
                            );
                            cond = Some(match cond {
                                Some(c) => Expr::Bin(Box::new(c), "and", Box::new(eq)),
                                None => eq,
                            });
                        }
                        cond
                    } else {
                        return Err("expected ON after JOIN".into());
                    };
                    joins.push(Join { kind, table, on });
                }
                from_items.push(FromItem { base, joins });
                if self.op(",") {
                    continue;
                }
                break;
            }
        }

        let where_ = if self.kw("where") {
            Some(self.expr()?)
        } else {
            None
        };
        let mut group_by = Vec::new();
        if self.kw("group") {
            if !self.kw("by") {
                return Err("expected BY after GROUP".into());
            }
            loop {
                group_by.push(self.expr()?);
                if self.op(",") {
                    continue;
                }
                break;
            }
        }
        let having = if self.kw("having") {
            Some(self.expr()?)
        } else {
            None
        };
        // WINDOW w AS (spec) [, ...] — after HAVING, before ORDER BY (PG)
        if self.kw("window") {
            loop {
                let wname = self.ident("window name")?;
                if !self.kw("as") {
                    return Err("expected AS after window name".into());
                }
                if !self.op("(") {
                    return Err("expected ( after window name AS".into());
                }
                let mut spec = self.window_spec_body()?;
                spec.name = Some(wname.clone());
                if !self.op(")") {
                    return Err("expected ) after window specification".into());
                }
                windows.push((wname, spec));
                if self.op(",") {
                    continue;
                }
                break;
            }
        }
        let mut order_by = Vec::new();
        if self.kw("order") {
            if !self.kw("by") {
                return Err("expected BY after ORDER".into());
            }
            loop {
                let e = self.expr()?;
                let mut desc = false;
                if self.kw("desc") {
                    desc = true;
                } else {
                    self.kw("asc");
                }
                // PG defaults: ASC => NULLS LAST, DESC => NULLS FIRST
                let mut nulls_first = desc;
                if self.kw("nulls") {
                    if self.kw("first") {
                        nulls_first = true;
                    } else if self.kw("last") {
                        nulls_first = false;
                    } else {
                        return Err("expected FIRST or LAST after NULLS".into());
                    }
                }
                order_by.push((e, desc, nulls_first));
                if self.op(",") {
                    continue;
                }
                break;
            }
        }
        let mut limit = None;
        let mut offset = None;
        loop {
            if limit.is_none() && self.kw("limit") {
                limit = Some(self.expr()?);
            } else if offset.is_none() && self.kw("offset") {
                offset = Some(self.expr()?);
            } else {
                break;
            }
        }
        Ok(SelectStmt {
            distinct,
            windows,
            items,
            from_items,
            where_,
            group_by,
            having,
            order_by,
            limit,
            offset,
        })
    }

    // precedence: OR < AND < NOT < predicate < add < mul < unary < postfix
    fn expr(&mut self) -> Result<Expr, String> {
        self.or_expr()
    }

    fn or_expr(&mut self) -> Result<Expr, String> {
        let mut l = self.and_expr()?;
        while self.kw("or") {
            let r = self.and_expr()?;
            l = Expr::Bin(Box::new(l), "OR", Box::new(r));
        }
        Ok(l)
    }

    fn and_expr(&mut self) -> Result<Expr, String> {
        let mut l = self.not_expr()?;
        while self.kw("and") {
            let r = self.not_expr()?;
            l = Expr::Bin(Box::new(l), "AND", Box::new(r));
        }
        Ok(l)
    }

    fn not_expr(&mut self) -> Result<Expr, String> {
        if self.kw("not") {
            return Ok(Expr::Not(Box::new(self.not_expr()?)));
        }
        self.predicate()
    }

    fn predicate(&mut self) -> Result<Expr, String> {
        let l = self.add_expr()?;
        // NOT LIKE / NOT IN / NOT BETWEEN
        if self.peek_kw("not") && !matches!(self.peek(), Tok::Op(_)) {
            // lookahead: only treat as postfix-NOT if LIKE/IN/BETWEEN follows
            let save = self.pos;
            self.next(); // NOT
            if self.kw("like") {
                let r = self.add_expr()?;
                return Ok(Expr::Like(Box::new(l), Box::new(r), true));
            }
            if self.kw("in") {
                return self.finish_in(l, true);
            }
            if self.kw("between") {
                return self.finish_between(l, true);
            }
            self.pos = save; // plain prefix NOT (handled by not_expr on next round)
        }
        if self.kw("is") {
            let neg = self.kw("not");
            if self.kw("null") {
                return Ok(Expr::IsNull(Box::new(l), neg));
            }
            return Err("expected NULL after IS".into());
        }
        if self.kw("like") {
            let r = self.add_expr()?;
            return Ok(Expr::Like(Box::new(l), Box::new(r), false));
        }
        if self.kw("in") {
            return self.finish_in(l, false);
        }
        if self.kw("between") {
            return self.finish_between(l, false);
        }
        let ops = ["~", "!~", "<=", ">=", "<>", "!=", "=", "<", ">"];
        // OPERATOR(schema.op) — explicit operator syntax (psql emits it)
        if matches!(self.peek(), Tok::Ident(id) if id == "operator")
            && matches!(self.peek_at(1), Tok::Op("("))
        {
            self.next(); // operator
            self.next(); // (
            let mut explicit: Option<&'static str> = None;
            loop {
                match self.next() {
                    Tok::Op(")") => break,
                    Tok::Op(".") => {} // schema separator inside OPERATOR()
                    Tok::Op(o) => explicit = Some(o),
                    Tok::Ident(_) => {}
                    t => return Err(format!("bad OPERATOR() syntax: {:?}", t)),
                }
            }
            let o = explicit.ok_or("OPERATOR() without an operator symbol")?;
            if !ops.contains(&o) {
                return Err(format!("unsupported operator {}", o));
            }
            let r = self.add_expr()?;
            return Ok(Expr::Bin(Box::new(l), o, Box::new(r)));
        }
        for o in ops {
            if self.op(o) {
                let r = self.add_expr()?;
                let stat: &'static str = match o {
                    "<=" | ">=" | "<>" | "!=" | "=" | "<" | ">" | "~" | "!~" => o,
                    _ => ">",
                };
                return Ok(Expr::Bin(Box::new(l), stat, Box::new(r)));
            }
        }
        Ok(l)
    }

    fn finish_in(&mut self, l: Expr, neg: bool) -> Result<Expr, String> {
        if !self.op("(") {
            return Err("expected ( after IN".into());
        }
        // IN (SELECT ...): uncorrelated subquery, lowered at rewrite
        // time; a UNION body is rejected with a clean error (the lowered
        // form is a single SelectStmt)
        if self.peek_kw("select") {
            self.next(); // SELECT keyword (select_stmt expects it gone)
            let first = self.select_stmt()?;
            if self.peek_kw("union") {
                return Err("UNION inside an IN-subquery is not supported".into());
            }
            if !self.op(")") {
                return Err("expected ) after IN subquery".into());
            }
            return Ok(Expr::InSub(Box::new(l), Box::new(first), neg));
        }
        let mut list = Vec::new();
        loop {
            list.push(self.expr()?);
            if self.op(",") {
                continue;
            }
            break;
        }
        if !self.op(")") {
            return Err("expected ) after IN list".into());
        }
        Ok(Expr::InList(Box::new(l), list, neg))
    }

    fn finish_between(&mut self, l: Expr, neg: bool) -> Result<Expr, String> {
        let lo = self.add_expr()?;
        if !self.kw("and") {
            return Err("expected AND in BETWEEN".into());
        }
        let hi = self.add_expr()?;
        Ok(Expr::Between(Box::new(l), Box::new(lo), Box::new(hi), neg))
    }

    fn add_expr(&mut self) -> Result<Expr, String> {
        let mut l = self.mul_expr()?;
        loop {
            if self.op("+") {
                let r = self.mul_expr()?;
                l = Expr::Bin(Box::new(l), "+", Box::new(r));
            } else if self.op("-") {
                let r = self.mul_expr()?;
                l = Expr::Bin(Box::new(l), "-", Box::new(r));
            } else if self.op("||") {
                let r = self.mul_expr()?;
                l = Expr::Bin(Box::new(l), "||", Box::new(r));
            } else {
                break;
            }
        }
        Ok(l)
    }

    fn mul_expr(&mut self) -> Result<Expr, String> {
        let mut l = self.unary_expr()?;
        loop {
            if self.op("*") {
                let r = self.unary_expr()?;
                l = Expr::Bin(Box::new(l), "*", Box::new(r));
            } else if self.op("/") {
                let r = self.unary_expr()?;
                l = Expr::Bin(Box::new(l), "/", Box::new(r));
            } else if self.op("%") {
                let r = self.unary_expr()?;
                l = Expr::Bin(Box::new(l), "%", Box::new(r));
            } else {
                break;
            }
        }
        Ok(l)
    }

    fn unary_expr(&mut self) -> Result<Expr, String> {
        if self.op("-") {
            return Ok(Expr::Neg(Box::new(self.unary_expr()?)));
        }
        if self.op("+") {
            return self.unary_expr();
        }
        self.postfix()
    }

    fn postfix(&mut self) -> Result<Expr, String> {
        let mut e = self.primary()?;
        while self.op("::") {
            let ty = self.parse_type()?;
            e = Expr::Cast(Box::new(e), ty);
        }
        Ok(e)
    }

    fn primary(&mut self) -> Result<Expr, String> {
        let mut e = self.primary_atom()?;
        // array subscript a[i] — parsed for catalog-tool compatibility;
        // evaluation is not supported and errors cleanly (catalog probes
        // only carry it inside subqueries over empty relations)
        while self.op("[") {
            let sub = self.expr()?;
            if !self.op("]") {
                return Err("expected ] after subscript".into());
            }
            e = Expr::Bin(Box::new(e), "[]", Box::new(sub));
        }
        // COLLATE <collation> — accepted and ignored (default collation)
        while self.peek_kw("collate") {
            self.next();
            let _ = self.ident("collation name")?;
            while self.op(".") {
                let _ = self.ident("collation name")?;
            }
        }
        Ok(e)
    }

    fn primary_atom(&mut self) -> Result<Expr, String> {
        self.depth += 1;
        if self.depth > MAX_EXPR_DEPTH {
            self.depth -= 1;
            return Err(format!(
                "statement too complex: expression nesting exceeds {} levels",
                MAX_EXPR_DEPTH
            ));
        }
        let r = self.primary_atom_inner();
        self.depth -= 1;
        r
    }

    fn primary_atom_inner(&mut self) -> Result<Expr, String> {
        match self.next() {
            Tok::Num(n) => {
                if n.contains('.') || n.contains('e') || n.contains('E') {
                    match n.parse::<f64>() {
                        Ok(f) => Ok(Expr::Lit(Value::Float(f))),
                        Err(_) => Err(format!("bad number '{}'", n)),
                    }
                } else {
                    match n.parse::<i64>() {
                        Ok(i) => Ok(Expr::Lit(Value::Int(i))),
                        Err(_) => match n.parse::<f64>() {
                            Ok(f) => Ok(Expr::Lit(Value::Float(f))),
                            Err(_) => Err(format!("bad number '{}'", n)),
                        },
                    }
                }
            }
            Tok::Str(s) => Ok(Expr::Lit(Value::Text(s))),
            Tok::Param(n) => Ok(Expr::Param(n as i32)),
            Tok::Op("(") => {
                if self.peek_kw("select") {
                    self.next(); // SELECT keyword (select_stmt expects it gone)
                    let sel = self.select_stmt()?;
                    if !self.op(")") {
                        return Err("expected ) after subquery".into());
                    }
                    return Ok(Expr::Subquery(Box::new(sel)));
                }
                let e = self.expr()?;
                if !self.op(")") {
                    return Err("expected )".into());
                }
                Ok(e)
            }
            Tok::Op("*") => Err("misplaced *".into()),
            Tok::Ident(s) => self.ident_expr(s),
            t => Err(format!("unexpected token {:?}", t)),
        }
    }

    fn ident_expr(&mut self, s: String) -> Result<Expr, String> {
        match s.as_str() {
            "true" => return Ok(Expr::Lit(Value::Bool(true))),
            "false" => return Ok(Expr::Lit(Value::Bool(false))),
            "null" => return Ok(Expr::Lit(Value::Null)),
            "case" => return self.case_expr(),
            // SQL-standard no-parentheses special registers (the paren-
            // thesized call forms fall through to the normal function path)
            "current_user" | "session_user" | "current_role"
                if !matches!(self.peek(), Tok::Op("(")) =>
            {
                return Ok(Expr::Func("current_user".into(), Vec::new()))
            }
            "current_schema" if !matches!(self.peek(), Tok::Op("(")) => {
                return Ok(Expr::Func("current_schema".into(), Vec::new()))
            }
            "current_database" if !matches!(self.peek(), Tok::Op("(")) => {
                return Ok(Expr::Func("current_database".into(), Vec::new()))
            }
            // typed literals: DATE '...' / TIMESTAMP '...' / TIME '...' /
            // INTERVAL '...'
            "date" | "timestamp" | "time" | "interval"
                if matches!(self.peek(), Tok::Str(_)) =>
            {
                let lit = self.next();
                let Tok::Str(text) = lit else {
                    return Err("expected string literal".into());
                };
                let v = match s.as_str() {
                    "date" => crate::temporal::parse_date(&text).map(Value::Date),
                    "time" => crate::temporal::parse_time(&text).map(Value::Time),
                    "timestamp" => {
                        crate::temporal::parse_timestamp(&text).map(Value::Timestamp)
                    }
                    _ => crate::temporal::parse_interval(&text).map(
                        |(months, days, micros)| Value::Interval { months, days, micros },
                    ),
                };
                return Ok(Expr::Lit(v?));
            }
            "current_timestamp" if !matches!(self.peek(), Tok::Op("(")) => {
                return Ok(Expr::Func("now".into(), Vec::new()))
            }
            "current_date" if !matches!(self.peek(), Tok::Op("(")) => {
                return Ok(Expr::Func("current_date".into(), Vec::new()))
            }
            "current_time" if !matches!(self.peek(), Tok::Op("(")) => {
                return Ok(Expr::Func("current_time".into(), Vec::new()))
            }
            // EXTRACT(field FROM expr) — special argument syntax
            "extract" if matches!(self.peek(), Tok::Op("(")) => {
                self.next(); // (
                let field = self.ident("extract field")?;
                if !self.kw("from") {
                    return Err("expected FROM in EXTRACT".into());
                }
                let e = self.expr()?;
                if !self.op(")") {
                    return Err("expected ) after EXTRACT".into());
                }
                return Ok(Expr::Func(
                    "__extract".into(),
                    vec![Expr::Lit(Value::Text(field)), e],
                ));
            }
            "cast"
                if self.op("(") => {
                    let e = self.expr()?;
                    if !self.kw("as") {
                        return Err("expected AS in CAST".into());
                    }
                    let ty = self.parse_type()?;
                    if !self.op(")") {
                        return Err("expected ) after CAST".into());
                    }
                    return Ok(Expr::Cast(Box::new(e), ty));
                }
            _ => {}
        }
        // function or aggregate call?
        if matches!(self.peek(), Tok::Op("(")) {
            let fname = s.clone();
            self.next(); // (
            // ARRAY(SELECT ...) and friends: a subquery as the sole argument
            if self.peek_kw("select") {
                self.next(); // SELECT keyword
                let sel = self.select_stmt()?;
                if !self.op(")") {
                    return Err("expected ) after subquery".into());
                }
                return Ok(Expr::Func(fname, vec![Expr::Subquery(Box::new(sel))]));
            }
            if matches!(fname.as_str(), "count" | "sum" | "min" | "max" | "avg") {
                let kind = match fname.as_str() {
                    "count" => AggKind::Count,
                    "sum" => AggKind::Sum,
                    "min" => AggKind::Min,
                    "max" => AggKind::Max,
                    _ => AggKind::Avg,
                };
                if self.op("*") {
                    if !self.op(")") {
                        return Err("expected ) after count(*)".into());
                    }
                    let filter = self.filter_clause()?.map(Box::new);
                    if self.peek_kw("over") {
                        self.next(); // OVER
                        let spec = self.over_clause()?;
                        return Ok(Expr::Win {
                            kind: WinKind::Agg(AggKind::CountStar),
                            args: vec![Expr::Lit(Value::Int(1))],
                            spec: Box::new(spec),
                            filter,
                        });
                    }
                    return Ok(Expr::Agg(
                        AggKind::CountStar,
                        Box::new(Expr::Lit(Value::Int(1))),
                        false,
                        filter,
                    ));
                }
                let distinct = self.kw("distinct");
                let arg = self.expr()?;
                if !self.op(")") {
                    return Err("expected ) after aggregate argument".into());
                }
                let filter = self.filter_clause()?.map(Box::new);
                if self.peek_kw("over") {
                    if distinct {
                        return Err("DISTINCT is not allowed in a window aggregate".into());
                    }
                    self.next(); // OVER
                    let spec = self.over_clause()?;
                    return Ok(Expr::Win {
                        kind: WinKind::Agg(kind),
                        args: vec![arg],
                        spec: Box::new(spec),
                        filter,
                    });
                }
                return Ok(Expr::Agg(kind, Box::new(arg), distinct, filter));
            }
            // scalar function
            let mut args = Vec::new();
            if !self.op(")") {
                loop {
                    args.push(self.expr()?);
                    if self.op(",") {
                        continue;
                    }
                    break;
                }
                if !self.op(")") {
                    return Err("expected ) after function arguments".into());
                }
            }
            // FILTER syntactically precedes OVER and applies to aggregates
            // only: catch it here so row_number() FILTER ... gives a proper
            // error instead of a generic syntax error
            let pre_filter = self.filter_clause()?;
            if pre_filter.is_some() && !matches!(
                fname.as_str(),
                "count" | "sum" | "avg" | "min" | "max"
            ) {
                return Err("FILTER is not allowed on non-aggregate window functions".into());
            }
            if self.peek_kw("over") {
                self.next(); // OVER
                let kind = match fname.as_str() {
                    "row_number" => WinKind::RowNumber,
                    "rank" => WinKind::Rank,
                    "dense_rank" => WinKind::DenseRank,
                    "percent_rank" => WinKind::PercentRank,
                    "cume_dist" => WinKind::CumeDist,
                    "ntile" => WinKind::Ntile,
                    "lag" => WinKind::Lag,
                    "lead" => WinKind::Lead,
                    "first_value" => WinKind::FirstValue,
                    "last_value" => WinKind::LastValue,
                    "nth_value" => WinKind::NthValue,
                    _ => {
                        return Err(format!(
                            "function \"{}\" is not a window function",
                            fname
                        ))
                    }
                };
                let spec = self.over_clause()?;
                return Ok(Expr::Win {
                    kind,
                    args,
                    spec: Box::new(spec),
                    filter: None,
                });
            }
            return Ok(Expr::Func(fname, args));
        }
        // qualified column t.col — or a schema-qualified function call
        // pg_catalog.pg_get_userbyid(...): if a ( follows the dotted name,
        // it is a function (schemas are ignored: our functions are global)
        if matches!(self.peek(), Tok::Op(".")) {
            self.next(); // .
            let second = self.ident("column name")?;
            if matches!(self.peek(), Tok::Op("(")) && second != "pg_catalog" {
                return self.ident_expr(second); // parse the call, drop schema
            }
            return Ok(Expr::QCol(s, second));
        }
        Ok(Expr::Col(s))
    }

    fn case_expr(&mut self) -> Result<Expr, String> {
        let operand = if self.peek_kw("when") {
            None
        } else {
            Some(Box::new(self.expr()?))
        };
        let mut when_then = Vec::new();
        while self.kw("when") {
            let w = self.expr()?;
            if !self.kw("then") {
                return Err("expected THEN in CASE".into());
            }
            let t = self.expr()?;
            when_then.push((w, t));
        }
        if when_then.is_empty() {
            return Err("CASE requires at least one WHEN".into());
        }
        let else_ = if self.kw("else") {
            Some(Box::new(self.expr()?))
        } else {
            None
        };
        if !self.kw("end") {
            return Err("expected END after CASE".into());
        }
        Ok(Expr::Case {
            operand,
            when_then,
            else_,
        })
    }
}



// ------------------------------------------------- constraint rendering --
// Renders a DEFAULT / CHECK expression back to SQL text so constraints can
// round-trip through the manifest and the WAL (re-parsed on recovery).

fn ty_sql(ty: &Ty) -> &'static str {
    match ty {
        Ty::Int => "int",
        Ty::Float => "float",
        Ty::Bool => "bool",
        Ty::Text => "text",
        Ty::Date => "date",
        Ty::Time => "time",
        Ty::Timestamp => "timestamp",
        Ty::Interval => "interval",
    }
}

fn lit_sql(v: &Value) -> String {
    match v {
        Value::Null => "NULL".into(),
        Value::Bool(b) => if *b { "true".into() } else { "false".into() },
        Value::Int(i) => i.to_string(),
        Value::Float(f) => {
            if f.is_finite() { format!("{}", f) } else { "NULL".into() }
        }
        Value::Text(t) => format!("'{}'", t.replace('\'', "''")),
        Value::Date(d) => format!("DATE '{}'", d),
        Value::Time(t) => format!("TIME '{}'", t),
        Value::Timestamp(ts) => format!("TIMESTAMP '{}'", ts),
        Value::Interval { .. } => "INTERVAL '0'".into(),
    }
}

/// SQL text for a constraint expression (a parseable subset is enough:
/// DEFAULT and CHECK bodies are ordinary scalar expressions).
pub fn expr_to_sql(e: &Expr) -> String {
    match e {
        Expr::Lit(v) => lit_sql(v),
        Expr::Col(c) => c.clone(),
        Expr::QCol(t, c) => format!("{}.{}", t, c),
        Expr::Param(n) => format!("${}", n),
        Expr::AggRef(i) => format!("__agg{}", i),
        Expr::Subquery(_) => "(SELECT ...)".into(),
        Expr::Neg(a) => format!("(-{})", expr_to_sql(a)),
        Expr::Not(a) => format!("(NOT {})", expr_to_sql(a)),
        Expr::IsNull(a, n) => format!(
            "({} IS {}NULL)",
            expr_to_sql(a),
            if *n { "" } else { "NOT " }
        ),
        Expr::Cast(a, t) => format!("({}::{})", expr_to_sql(a), ty_sql(t)),
        Expr::Like(a, b, n) => format!(
            "({} {}LIKE {})",
            expr_to_sql(a),
            if *n { "NOT " } else { "" },
            expr_to_sql(b)
        ),
        Expr::Between(a, lo, hi, n) => format!(
            "({} {}BETWEEN {} AND {})",
            expr_to_sql(a),
            if *n { "NOT " } else { "" },
            expr_to_sql(lo),
            expr_to_sql(hi)
        ),
        Expr::InList(a, l, n) => format!(
            "({} {}IN ({}))",
            expr_to_sql(a),
            if *n { "NOT " } else { "" },
            l.iter().map(expr_to_sql).collect::<Vec<_>>().join(", ")
        ),
        Expr::InSub(a, _, n) => format!(
            "({} {}IN (SELECT ...))",
            expr_to_sql(a),
            if *n { "NOT " } else { "" }
        ),
        Expr::Bin(a, op, b) => {
            format!("({} {} {})", expr_to_sql(a), op, expr_to_sql(b))
        }
        Expr::Case {
            operand,
            when_then,
            else_,
        } => {
            let mut out = String::from("CASE");
            if let Some(o) = operand {
                out.push(' ');
                out.push_str(&expr_to_sql(o));
            }
            for (w, t) in when_then {
                out.push_str(" WHEN ");
                out.push_str(&expr_to_sql(w));
                out.push_str(" THEN ");
                out.push_str(&expr_to_sql(t));
            }
            if let Some(x) = else_ {
                out.push_str(" ELSE ");
                out.push_str(&expr_to_sql(x));
            }
            out.push_str(" END");
            out
        }
        Expr::Func(f, args) => format!(
            "{}({})",
            f,
            args.iter().map(expr_to_sql).collect::<Vec<_>>().join(", ")
        ),
        Expr::Agg(kind, a, distinct, filter) => {
            let kn = match kind {
                AggKind::CountStar => "count",
                AggKind::Count => "count",
                AggKind::Sum => "sum",
                AggKind::Avg => "avg",
                AggKind::Min => "min",
                AggKind::Max => "max",
            };
            let mut out = format!(
                "{}({}{})",
                kn,
                if *distinct { "DISTINCT " } else { "" },
                expr_to_sql(a)
            );
            if let Some(f) = filter {
                out.push_str(&format!(" FILTER (WHERE {})", expr_to_sql(f)));
            }
            out
        }
        Expr::Win {
            kind,
            args,
            spec,
            filter,
        } => {
            let _ = filter;
            format!(
                "{}({}) OVER ({})",
                match kind {
                    WinKind::RowNumber => "row_number",
                    WinKind::Rank => "rank",
                    WinKind::DenseRank => "dense_rank",
                    WinKind::PercentRank => "percent_rank",
                    WinKind::CumeDist => "cume_dist",
                    WinKind::Ntile => "ntile",
                    WinKind::Lag => "lag",
                    WinKind::Lead => "lead",
                    WinKind::FirstValue => "first_value",
                    WinKind::LastValue => "last_value",
                    WinKind::NthValue => "nth_value",
                    WinKind::Agg(_) => "sum",
                },
                args.iter().map(expr_to_sql).collect::<Vec<_>>().join(", "),
                spec.partition_by
                    .iter()
                    .map(expr_to_sql)
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        }
    }
}

/// Parses a bare expression from SQL text (constraint round-trip through
/// the WAL/manifest: DEFAULT and CHECK bodies).
pub fn parse_expr_text(s: &str) -> Option<Expr> {
    let toks = lex(&format!("SELECT {}", s)).ok()?;
    let mut p = Parser::new(toks);
    let stmt = p.parse_one().ok()??;
    match stmt {
        Stmt::Select(sel) => match sel.items.into_iter().next()? {
            SelectItem::Expr(e, _) => Some(e),
            _ => None,
        },
        _ => None,
    }
}

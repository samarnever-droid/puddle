//! Vectorized batch executor kernels (X100-style, see PAPERS.md #4/#5).
//!
//! Expressions evaluate over whole columns of a `VSrc` for a selection of
//! row indices — no per-row scope resolution, no per-row context structs.
//! CASE branches stay lazy per branch subset so error semantics match the
//! reference executor. Equi-joins run as hash joins (`hash_join`).
//!
//! The reference executor (engine.rs) stays as the correctness oracle for
//! differential testing; anything this module doesn't cover falls back to it.

use crate::engine::{
    compare, coerce, eval_between, eval_bin, eval_func, eval_in, eval_like, gkey, GK, PqError,
    Scope, Value,
};
use crate::sql::Expr;
use std::collections::HashMap;

/// A column source: either borrowed table columns (single-table scans —
/// zero materialization) or owned materialized columns (post-join).
pub enum VSrc<'a> {
    Borrowed { cols: Vec<&'a [Value]>, n: usize },
    Owned { cols: Vec<Vec<Value>> },
}

impl<'a> VSrc<'a> {
    pub fn col(&self, i: usize) -> &[Value] {
        match self {
            VSrc::Borrowed { cols, .. } => cols.get(i).map_or(&[], |c| *c),
            VSrc::Owned { cols } => cols.get(i).map_or(&[], |c| &c[..]),
        }
    }
    pub fn width(&self) -> usize {
        match self {
            VSrc::Borrowed { cols, .. } => cols.len(),
            VSrc::Owned { cols } => cols.len(),
        }
    }
    pub fn nrows(&self) -> usize {
        match self {
            VSrc::Borrowed { n, .. } => *n,
            VSrc::Owned { cols } => cols.first().map_or(0, |c| c.len()),
        }
    }
    /// Materializes row `i` (used for group representative rows).
    pub fn row(&self, i: usize) -> Vec<Value> {
        (0..self.width()).map(|c| self.col(c)[i].clone()).collect()
    }
}

type R<T> = Result<T, PqError>;

/// Evaluates `e` for every row index in `sel` against `src`.
pub fn veval(
    e: &Expr,
    src: &VSrc,
    sel: &[usize],
    scope: &Scope,
    params: &[Value],
) -> R<Vec<Value>> {
    let n = sel.len();
    match e {
        Expr::Lit(v) => Ok(vec![v.clone(); n]),
        // subqueries are rewritten to literals before vectorized evaluation
        Expr::Subquery(_) => Err(PqError::new(
            "XX000",
            "internal error: unevaluated scalar subquery",
        )),
        Expr::Col(name) => {
            let idx = scope.resolve(name)?;
            let col = src.col(idx);
            Ok(sel.iter().map(|&i| col[i].clone()).collect())
        }
        Expr::QCol(t, c) => {
            let idx = scope.resolve_q(t, c)?;
            let col = src.col(idx);
            Ok(sel.iter().map(|&i| col[i].clone()).collect())
        }
        Expr::Param(p) => {
            if *p < 1 || (*p as usize) > params.len() {
                return Err(PqError::new(
                    "42P02",
                    format!("there is no parameter ${}", p),
                ));
            }
            Ok(vec![params[(*p - 1) as usize].clone(); n])
        }
        Expr::AggRef(_) | Expr::Agg(..) => Err(PqError::new(
            "42803",
            "aggregate functions are not allowed here",
        )),
        Expr::Neg(inner) => {
            let v = veval(inner, src, sel, scope, params)?;
            let mut out = Vec::with_capacity(n);
            for x in v {
                out.push(match x {
                    Value::Int(i) => Value::Int(-i),
                    Value::Float(f) => Value::Float(-f),
                    Value::Null => Value::Null,
                    ref xv => {
                        return Err(PqError::wrong_type(format!(
                            "cannot negate type {}",
                            crate::engine::type_name(xv)
                        )))
                    }
                });
            }
            Ok(out)
        }
        Expr::Not(inner) => {
            let v = veval(inner, src, sel, scope, params)?;
            let mut out = Vec::with_capacity(n);
            for x in v {
                out.push(match x {
                    Value::Bool(b) => Value::Bool(!b),
                    Value::Null => Value::Null,
                    ref xv => {
                        return Err(PqError::wrong_type(format!(
                            "argument of NOT must be boolean, not {}",
                            crate::engine::type_name(xv)
                        )))
                    }
                });
            }
            Ok(out)
        }
        Expr::Bin(l, op, r) => {
            let lv = veval(l, src, sel, scope, params)?;
            let rv = veval(r, src, sel, scope, params)?;
            let mut out = Vec::with_capacity(n);
            for k in 0..n {
                out.push(eval_bin(op, &lv[k], &rv[k])?);
            }
            Ok(out)
        }
        Expr::IsNull(inner, neg) => {
            let v = veval(inner, src, sel, scope, params)?;
            Ok(v.into_iter()
                .map(|x| {
                    let is_null = matches!(x, Value::Null);
                    Value::Bool(if *neg { !is_null } else { is_null })
                })
                .collect())
        }
        Expr::Like(l, r, neg) => {
            let lv = veval(l, src, sel, scope, params)?;
            let rv = veval(r, src, sel, scope, params)?;
            let mut out = Vec::with_capacity(n);
            for k in 0..n {
                out.push(eval_like(&lv[k], &rv[k], *neg)?);
            }
            Ok(out)
        }
        Expr::InList(l, list, neg) => {
            let lv = veval(l, src, sel, scope, params)?;
            let mut lists = Vec::with_capacity(list.len());
            for e in list {
                lists.push(veval(e, src, sel, scope, params)?);
            }
            let mut out = Vec::with_capacity(n);
            for k in 0..n {
                let vals: Vec<Value> = lists.iter().map(|v| v[k].clone()).collect();
                out.push(eval_in(&lv[k], &vals, *neg)?);
            }
            Ok(out)
        }
        Expr::Between(x, lo, hi, neg) => {
            let xv = veval(x, src, sel, scope, params)?;
            let lov = veval(lo, src, sel, scope, params)?;
            let hiv = veval(hi, src, sel, scope, params)?;
            let mut out = Vec::with_capacity(n);
            for k in 0..n {
                out.push(eval_between(&xv[k], &lov[k], &hiv[k], *neg)?);
            }
            Ok(out)
        }
        Expr::Case { operand, when_then, else_ } => {
            let opv = match operand {
                Some(o) => Some(veval(o, src, sel, scope, params)?),
                None => None,
            };
            let mut out: Vec<Value> = vec![Value::Null; n];
            let mut done = vec![false; n];
            for (w, t) in when_then {
                // Positions not yet resolved. WHEN conditions are evaluated
                // ONLY over these rows — matching the reference executor's
                // per-row laziness: a row that already matched an earlier
                // WHEN must never trigger an error in a later WHEN.
                let pend: Vec<usize> = (0..n).filter(|&k| !done[k]).collect();
                if pend.is_empty() {
                    break;
                }
                let prows: Vec<usize> = pend.iter().map(|&k| sel[k]).collect();
                let wv = veval(w, src, &prows, scope, params)?;
                let sub: Vec<usize> = pend
                    .iter()
                    .enumerate()
                    .filter(|(j, &k)| {
                        
                        match &opv {
                            Some(ov) => {
                                compare(&ov[k], &wv[*j]).map(|c| c == Some(0)).unwrap_or(false)
                            }
                            None => matches!(wv[*j], Value::Bool(true)),
                        }
                    })
                    .map(|(_, k)| *k)
                    .collect();
                if sub.is_empty() {
                    continue;
                }
                let rows: Vec<usize> = sub.iter().map(|&k| sel[k]).collect();
                let tv = veval(t, src, &rows, scope, params)?;
                for (j, &k) in sub.iter().enumerate() {
                    out[k] = tv[j].clone();
                    done[k] = true;
                }
            }
            // ELSE for the rest (default Null already in place)
            let rem: Vec<usize> = (0..n).filter(|&k| !done[k]).collect();
            if !rem.is_empty() && else_.is_some() {
                let rows: Vec<usize> = rem.iter().map(|&k| sel[k]).collect();
                let ev = veval(else_.as_ref().unwrap(), src, &rows, scope, params)?;
                for (j, &k) in rem.iter().enumerate() {
                    out[k] = ev[j].clone();
                    done[k] = true;
                }
            }
            Ok(out)
        }
        Expr::Cast(inner, ty) => {
            let v = veval(inner, src, sel, scope, params)?;
            let mut out = Vec::with_capacity(n);
            for x in v {
                out.push(coerce(x, *ty)?);
            }
            Ok(out)
        }
        Expr::Func(name, args) => {
            let mut argvs = Vec::with_capacity(args.len());
            for a in args {
                argvs.push(veval(a, src, sel, scope, params)?);
            }
            let mut out = Vec::with_capacity(n);
            for k in 0..n {
                let vals: Vec<Value> = argvs.iter().map(|v| v[k].clone()).collect();
                out.push(eval_func(name, &vals)?);
            }
            Ok(out)
        }
    }
}

// ------------------------------------------------------------- hash join --

/// Hash join: builds a key -> row-index map from the right side and probes
/// with the left keys. `left_keys[k][i]` is key part k for left row i.
/// Returns (left row, Option<right row>) pairs; `None` only for LEFT joins
/// without a match.
pub fn hash_join(
    nleft: usize,
    left_keys: &[Vec<Value>],
    right_keys: &[Vec<Value>],
    left_join: bool,
) -> Vec<(usize, Option<usize>)> {
    let nright = right_keys.first().map_or(0, |v| v.len());
    let mut map: HashMap<Vec<GK>, Vec<usize>> = HashMap::with_capacity(nright);
    for j in 0..nright {
        let key: Vec<GK> = right_keys.iter().map(|k| gkey(&k[j])).collect();
        // SQL: NULL = NULL is not true — null keys never join
        if key.iter().any(|k| matches!(k, GK::Null)) {
            continue;
        }
        map.entry(key).or_default().push(j);
    }
    let mut out = Vec::new();
    for i in 0..nleft {
        let key: Vec<GK> = left_keys.iter().map(|k| gkey(&k[i])).collect();
        if key.iter().any(|k| matches!(k, GK::Null)) {
            if left_join {
                out.push((i, None));
            }
            continue;
        }
        match map.get(&key) {
            Some(js) => {
                for &j in js {
                    out.push((i, Some(j)));
                }
            }
            None => {
                if left_join {
                    out.push((i, None));
                }
            }
        }
    }
    out
}

//! Evaluation of table/column check constraints.
//!
//! Kit check constraints are stored as serializable expression strings (the
//! TypeScript kit uses JS functions at the API layer, but the conformance
//! fixtures and cross-language schema serialize a string grammar). This module
//! implements a small, deterministic evaluator for that grammar so Rust,
//! Python, and TypeScript enforce the same constraints.
//!
//! Supported grammar:
//!
//! ```text
//! expr       := or_expr
//! or_expr    := and_expr ( ("OR" | "||") and_expr )*
//! and_expr   := not_expr ( ("AND" | "&&") not_expr )*
//! not_expr   := ("NOT" | "!") not_expr | atom
//! atom       := "(" or_expr ")" | comparison
//! comparison := operand OP operand
//! operand    := column | number | string | bool | null
//! OP         := "=" | "==" | "!=" | "<>" | "<" | "<=" | ">" | ">="
//! ```
//!
//! Evaluation follows SQL three-valued logic: a check is only *violated* when it
//! evaluates to a definite `false`. `NULL`/unknown comparisons pass.

use serde_json::{Map, Value};

/// Error produced when a check expression cannot be parsed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckParseError(pub String);

#[derive(Debug, Clone, PartialEq)]
pub enum CheckExpression {
    Compare {
        left: CheckOperand,
        op: CheckOperator,
        right: CheckOperand,
    },
    And(Box<CheckExpression>, Box<CheckExpression>),
    Or(Box<CheckExpression>, Box<CheckExpression>),
    Not(Box<CheckExpression>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum CheckOperand {
    Column(String),
    Number(f64),
    String(String),
    Bool(bool),
    Null,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckOperator {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

/// Evaluate a check expression against a row.
///
/// Returns `Ok(true)` when the check holds (including unknown/NULL results) and
/// `Ok(false)` only when the predicate is definitely violated.
pub fn eval_check(expr: &str, row: &Map<String, Value>) -> Result<bool, CheckParseError> {
    Ok(parse_check(expr)?.eval(row) != Some(false))
}

pub fn parse_check(expr: &str) -> Result<CheckExpression, CheckParseError> {
    let tokens = tokenize(expr)?;
    let mut parser = Parser { tokens, pos: 0 };
    let result = parser.parse_or()?;
    if parser.pos != parser.tokens.len() {
        return Err(CheckParseError(format!(
            "unexpected trailing tokens in check expression: {expr}"
        )));
    }
    Ok(result)
}

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Ident(String),
    Number(f64),
    Str(String),
    Bool(bool),
    Null,
    Op(String),
    And,
    Or,
    Not,
    LParen,
    RParen,
}

fn tokenize(expr: &str) -> Result<Vec<Token>, CheckParseError> {
    let chars: Vec<char> = expr.chars().collect();
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
            continue;
        }
        match c {
            '(' => {
                tokens.push(Token::LParen);
                i += 1;
            }
            ')' => {
                tokens.push(Token::RParen);
                i += 1;
            }
            '\'' | '"' => {
                let quote = c;
                i += 1;
                let mut s = String::new();
                while i < chars.len() && chars[i] != quote {
                    if chars[i] == '\\' && i + 1 < chars.len() {
                        s.push(chars[i + 1]);
                        i += 2;
                    } else {
                        s.push(chars[i]);
                        i += 1;
                    }
                }
                if i >= chars.len() {
                    return Err(CheckParseError("unterminated string literal".into()));
                }
                i += 1; // closing quote
                tokens.push(Token::Str(s));
            }
            // A bare `!` (not part of `!=`) is logical NOT, per the grammar.
            '!' if !(i + 1 < chars.len() && chars[i + 1] == '=') => {
                tokens.push(Token::Not);
                i += 1;
            }
            '=' | '!' | '<' | '>' => {
                let mut op = String::new();
                op.push(c);
                i += 1;
                if i < chars.len() && (chars[i] == '=' || (c == '<' && chars[i] == '>')) {
                    op.push(chars[i]);
                    i += 1;
                }
                tokens.push(Token::Op(op));
            }
            '&' if i + 1 < chars.len() && chars[i + 1] == '&' => {
                tokens.push(Token::And);
                i += 2;
            }
            '|' if i + 1 < chars.len() && chars[i + 1] == '|' => {
                tokens.push(Token::Or);
                i += 2;
            }
            _ if c.is_ascii_digit()
                || (c == '-' && i + 1 < chars.len() && chars[i + 1].is_ascii_digit()) =>
            {
                let mut num = String::new();
                num.push(c);
                i += 1;
                while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '.') {
                    num.push(chars[i]);
                    i += 1;
                }
                let value = num
                    .parse::<f64>()
                    .map_err(|_| CheckParseError(format!("invalid number: {num}")))?;
                tokens.push(Token::Number(value));
            }
            _ if c.is_alphabetic() || c == '_' => {
                let mut ident = String::new();
                while i < chars.len()
                    && (chars[i].is_alphanumeric() || chars[i] == '_' || chars[i] == '.')
                {
                    ident.push(chars[i]);
                    i += 1;
                }
                match ident.to_ascii_lowercase().as_str() {
                    "and" => tokens.push(Token::And),
                    "or" => tokens.push(Token::Or),
                    "not" => tokens.push(Token::Not),
                    "true" => tokens.push(Token::Bool(true)),
                    "false" => tokens.push(Token::Bool(false)),
                    "null" => tokens.push(Token::Null),
                    _ => tokens.push(Token::Ident(ident)),
                }
            }
            _ => return Err(CheckParseError(format!("unexpected character: {c}"))),
        }
    }
    Ok(tokens)
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

#[derive(Debug, Clone, PartialEq)]
enum Operand {
    Null,
    Num(f64),
    Str(String),
    Bool(bool),
}

impl Parser {
    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn parse_or(&mut self) -> Result<CheckExpression, CheckParseError> {
        let mut acc = self.parse_and()?;
        while matches!(self.peek(), Some(Token::Or)) {
            self.pos += 1;
            let rhs = self.parse_and()?;
            acc = CheckExpression::Or(Box::new(acc), Box::new(rhs));
        }
        Ok(acc)
    }

    fn parse_and(&mut self) -> Result<CheckExpression, CheckParseError> {
        let mut acc = self.parse_not()?;
        while matches!(self.peek(), Some(Token::And)) {
            self.pos += 1;
            let rhs = self.parse_not()?;
            acc = CheckExpression::And(Box::new(acc), Box::new(rhs));
        }
        Ok(acc)
    }

    fn parse_not(&mut self) -> Result<CheckExpression, CheckParseError> {
        if matches!(self.peek(), Some(Token::Not)) {
            self.pos += 1;
            return Ok(CheckExpression::Not(Box::new(self.parse_not()?)));
        }
        self.parse_atom()
    }

    fn parse_atom(&mut self) -> Result<CheckExpression, CheckParseError> {
        if matches!(self.peek(), Some(Token::LParen)) {
            self.pos += 1;
            let inner = self.parse_or()?;
            match self.peek() {
                Some(Token::RParen) => {
                    self.pos += 1;
                    Ok(inner)
                }
                _ => Err(CheckParseError("expected closing parenthesis".into())),
            }
        } else {
            self.parse_comparison()
        }
    }

    fn parse_comparison(&mut self) -> Result<CheckExpression, CheckParseError> {
        let left = self.parse_operand()?;
        let op = match self.peek() {
            Some(Token::Op(op)) => op.clone(),
            other => {
                return Err(CheckParseError(format!(
                    "expected comparison operator, found {other:?}"
                )))
            }
        };
        self.pos += 1;
        let right = self.parse_operand()?;
        let op = match op.as_str() {
            "=" | "==" => CheckOperator::Eq,
            "!=" | "<>" => CheckOperator::Ne,
            "<" => CheckOperator::Lt,
            "<=" => CheckOperator::Le,
            ">" => CheckOperator::Gt,
            ">=" => CheckOperator::Ge,
            _ => {
                return Err(CheckParseError(format!(
                    "unsupported comparison operator: {op}"
                )))
            }
        };
        Ok(CheckExpression::Compare { left, op, right })
    }

    fn parse_operand(&mut self) -> Result<CheckOperand, CheckParseError> {
        let tok = self
            .peek()
            .cloned()
            .ok_or_else(|| CheckParseError("unexpected end of expression".into()))?;
        self.pos += 1;
        Ok(match tok {
            Token::Number(n) => CheckOperand::Number(n),
            Token::Str(s) => CheckOperand::String(s),
            Token::Bool(b) => CheckOperand::Bool(b),
            Token::Null => CheckOperand::Null,
            Token::Ident(name) => CheckOperand::Column(name),
            other => {
                return Err(CheckParseError(format!(
                    "expected an operand, found {other:?}"
                )))
            }
        })
    }
}

impl CheckExpression {
    fn eval(&self, row: &Map<String, Value>) -> Option<bool> {
        match self {
            CheckExpression::Compare { left, op, right } => {
                compare(&left.resolve(row), *op, &right.resolve(row))
            }
            CheckExpression::And(left, right) => and3(left.eval(row), right.eval(row)),
            CheckExpression::Or(left, right) => or3(left.eval(row), right.eval(row)),
            CheckExpression::Not(expression) => not3(expression.eval(row)),
        }
    }
}

impl CheckOperand {
    fn resolve(&self, row: &Map<String, Value>) -> Operand {
        match self {
            CheckOperand::Column(name) => value_to_operand(row.get(name)),
            CheckOperand::Number(value) => Operand::Num(*value),
            CheckOperand::String(value) => Operand::Str(value.clone()),
            CheckOperand::Bool(value) => Operand::Bool(*value),
            CheckOperand::Null => Operand::Null,
        }
    }
}

fn value_to_operand(value: Option<&Value>) -> Operand {
    match value {
        None | Some(Value::Null) => Operand::Null,
        Some(Value::Bool(b)) => Operand::Bool(*b),
        Some(Value::Number(n)) => Operand::Num(n.as_f64().unwrap_or(f64::NAN)),
        Some(Value::String(s)) => Operand::Str(s.clone()),
        Some(other) => Operand::Str(other.to_string()),
    }
}

/// Compare two operands, returning `None` for unknown (NULL-involving) results.
fn compare(lhs: &Operand, op: CheckOperator, rhs: &Operand) -> Option<bool> {
    if matches!(lhs, Operand::Null) || matches!(rhs, Operand::Null) {
        return None;
    }
    let ord = match (lhs, rhs) {
        (Operand::Num(a), Operand::Num(b)) => a.partial_cmp(b),
        (Operand::Str(a), Operand::Str(b)) => Some(a.cmp(b)),
        (Operand::Bool(a), Operand::Bool(b)) => Some(a.cmp(b)),
        // Mixed types only support (in)equality and are otherwise unknown.
        _ => None,
    };
    match op {
        CheckOperator::Eq => Some(lhs == rhs),
        CheckOperator::Ne => Some(lhs != rhs),
        CheckOperator::Lt => ord.map(|o| o.is_lt()),
        CheckOperator::Le => ord.map(|o| o.is_le()),
        CheckOperator::Gt => ord.map(|o| o.is_gt()),
        CheckOperator::Ge => ord.map(|o| o.is_ge()),
    }
}

fn not3(v: Option<bool>) -> Option<bool> {
    v.map(|b| !b)
}

fn and3(a: Option<bool>, b: Option<bool>) -> Option<bool> {
    match (a, b) {
        (Some(false), _) | (_, Some(false)) => Some(false),
        (Some(true), Some(true)) => Some(true),
        _ => None,
    }
}

fn or3(a: Option<bool>, b: Option<bool>) -> Option<bool> {
    match (a, b) {
        (Some(true), _) | (_, Some(true)) => Some(true),
        (Some(false), Some(false)) => Some(false),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn row(v: Value) -> Map<String, Value> {
        v.as_object().unwrap().clone()
    }

    #[test]
    fn passes_simple_comparison() {
        let r = row(json!({ "age": 30 }));
        assert!(eval_check("age >= 0", &r).unwrap());
    }

    #[test]
    fn fails_simple_comparison() {
        let r = row(json!({ "age": -1 }));
        assert!(!eval_check("age >= 0", &r).unwrap());
    }

    #[test]
    fn null_comparison_passes() {
        let r = row(json!({ "age": null }));
        assert!(eval_check("age >= 0", &r).unwrap());
    }

    #[test]
    fn and_or_combination() {
        let r = row(json!({ "a": 5, "b": 10 }));
        assert!(eval_check("a > 0 AND b > 0", &r).unwrap());
        assert!(!eval_check("a > 0 AND b < 0", &r).unwrap());
        assert!(eval_check("a < 0 OR b > 0", &r).unwrap());
        assert!(!eval_check("a < 0 OR b < 0", &r).unwrap());
    }

    #[test]
    fn parentheses_and_not() {
        let r = row(json!({ "a": 5, "b": 10 }));
        assert!(eval_check("NOT (a > 100)", &r).unwrap());
        assert!(eval_check("(a = 5 OR a = 6) AND b = 10", &r).unwrap());
    }

    #[test]
    fn bang_is_logical_not() {
        let r = row(json!({ "a": 5 }));
        // `!` is logical NOT (distinct from `!=`).
        assert!(eval_check("!(a > 100)", &r).unwrap());
        assert!(!eval_check("!(a > 0)", &r).unwrap());
        assert!(eval_check("a != 6", &r).unwrap());
        assert!(!eval_check("a != 5", &r).unwrap());
    }

    #[test]
    fn string_and_cross_column() {
        let r = row(json!({ "lo": 1, "hi": 9, "kind": "x" }));
        assert!(eval_check("lo < hi", &r).unwrap());
        assert!(eval_check("kind = 'x'", &r).unwrap());
        assert!(!eval_check("kind != 'x'", &r).unwrap());
    }

    #[test]
    fn rejects_garbage() {
        let r = row(json!({}));
        assert!(eval_check("@@@", &r).is_err());
    }
}

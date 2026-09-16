use serde_json::{Map, Value};

use crate::instance::InstanceValues;

use super::{ManifestRenderError, TemplateText, TemplateTextPart};

/// The starting point for the token that stands in for an instance value.
const TOKEN_PREFIX: &str = "sleepypodsInstanceValue";

/// A raw manifest whose instance values wait outside the text until it parses.
///
/// The manifest is YAML a class author wrote, and its instance values come from
/// a tenant. Pasting a value into the text before the parse lets a value
/// carrying YAML punctuation close the author's scalar and open keys of its own,
/// so instead the text parses with a token in each value's position and
/// [`PlaceholderDocument::bind`] puts the values into the parsed document. Each
/// value then lands inside one scalar, and the document keeps the structure the
/// author wrote.
pub(super) struct PlaceholderDocument {
    text: String,
    prefix: String,
    /// Each token paired with the value that replaces it, in the order the
    /// template names them.
    substitutions: Vec<(String, String)>,
}

impl PlaceholderDocument {
    pub(super) fn new(
        template: &TemplateText,
        values: &InstanceValues,
    ) -> Result<Self, ManifestRenderError> {
        let prefix = unique_prefix(template);
        let mut text = String::new();
        let mut substitutions = Vec::new();
        for part in template.parts() {
            match part {
                TemplateTextPart::Literal(literal) => text.push_str(literal),
                TemplateTextPart::InstanceValue(field) => {
                    let value = values.get(field).ok_or_else(|| {
                        ManifestRenderError::MissingInstanceValue {
                            field: field.clone(),
                        }
                    })?;
                    let token = format!("{prefix}{}-", substitutions.len());
                    text.push_str(&token);
                    substitutions.push((token, value.clone()));
                }
            }
        }
        Ok(Self {
            text,
            prefix,
            substitutions,
        })
    }

    /// The manifest to parse, with a token wherever an instance value belongs.
    pub(super) fn text(&self) -> &str {
        &self.text
    }

    /// Replace every token in the parsed document with the value it stands for,
    /// covering map keys as well as scalars.
    pub(super) fn bind(&self, value: &mut Value) {
        if self.substitutions.is_empty() {
            return;
        }
        match value {
            Value::String(text) => {
                if let Some(bound) = self.substitute(text) {
                    *text = bound;
                }
            }
            Value::Array(items) => {
                for item in items {
                    self.bind(item);
                }
            }
            Value::Object(map) => {
                let mut bound = Map::with_capacity(map.len());
                for (key, mut item) in std::mem::take(map) {
                    self.bind(&mut item);
                    bound.insert(self.substitute(&key).unwrap_or(key), item);
                }
                *map = bound;
            }
            Value::Null | Value::Bool(_) | Value::Number(_) => {}
        }
    }

    /// `None` when the text holds no token and can stay as it is.
    fn substitute(&self, text: &str) -> Option<String> {
        if !text.contains(self.prefix.as_str()) {
            return None;
        }
        let mut bound = String::with_capacity(text.len());
        let mut rest = text;
        while let Some(start) = rest.find(self.prefix.as_str()) {
            let (before, candidate) = rest.split_at(start);
            bound.push_str(before);
            match self.token_at(candidate) {
                // Scanning continues past the value rather than through it, so a
                // value that happens to spell a token stays inert text.
                Some((value, remainder)) => {
                    bound.push_str(value);
                    rest = remainder;
                }
                None => {
                    bound.push_str(&candidate[..self.prefix.len()]);
                    rest = &candidate[self.prefix.len()..];
                }
            }
        }
        bound.push_str(rest);
        Some(bound)
    }

    /// The value and the text after it, when `text` opens with a token.
    fn token_at<'a>(&'a self, text: &'a str) -> Option<(&'a str, &'a str)> {
        self.substitutions.iter().find_map(|(token, value)| {
            text.strip_prefix(token.as_str())
                .map(|remainder| (value.as_str(), remainder))
        })
    }
}

/// A token has to be distinguishable from the manifest around it, so the prefix
/// grows until the author's literal text holds no copy of it.
fn unique_prefix(template: &TemplateText) -> String {
    let mut prefix = String::from(TOKEN_PREFIX);
    while template.parts().iter().any(|part| match part {
        TemplateTextPart::Literal(literal) => literal.contains(&prefix),
        TemplateTextPart::InstanceValue(_) => false,
    }) {
        prefix.push('x');
    }
    prefix
}

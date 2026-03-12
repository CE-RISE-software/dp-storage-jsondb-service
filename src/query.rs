use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::cmp::Ordering;
use utoipa::ToSchema;

use crate::error::AppError;

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct QueryRequest {
    pub filter: RecordQueryFilter,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct RecordQueryFilter {
    #[serde(rename = "where")]
    pub where_conditions: Vec<RecordQueryCondition>,
    #[serde(default)]
    pub sort: Vec<RecordQuerySort>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct RecordQueryCondition {
    pub field: String,
    pub op: QueryOperator,
    pub value: Value,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum QueryOperator {
    Eq,
    Ne,
    In,
    Contains,
    Exists,
    Gt,
    Gte,
    Lt,
    Lte,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct RecordQuerySort {
    pub field: String,
    pub direction: SortDirection,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum SortDirection {
    Asc,
    Desc,
}

#[derive(Clone, Debug)]
pub struct QueryRecord<'a> {
    pub id: &'a str,
    pub model: &'a str,
    pub version: &'a str,
    pub payload: &'a Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl RecordQueryFilter {
    pub fn validate(&self) -> Result<(), AppError> {
        if self.where_conditions.is_empty() {
            return Err(AppError::BadRequest(
                "query filter must include at least one condition".to_string(),
            ));
        }

        for condition in &self.where_conditions {
            if condition.field.trim().is_empty() {
                return Err(AppError::BadRequest(
                    "query condition field cannot be empty".to_string(),
                ));
            }

            if matches!(condition.op, QueryOperator::In) && !condition.value.is_array() {
                return Err(AppError::BadRequest(format!(
                    "operator `in` requires an array value for field `{}`",
                    condition.field
                )));
            }

            if matches!(condition.op, QueryOperator::Exists) && !condition.value.is_boolean() {
                return Err(AppError::BadRequest(format!(
                    "operator `exists` requires a boolean value for field `{}`",
                    condition.field
                )));
            }
        }

        Ok(())
    }

    pub fn matches(&self, record: &QueryRecord<'_>) -> Result<bool, AppError> {
        for condition in &self.where_conditions {
            if !condition.matches(record)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub fn compare(
        &self,
        left: &QueryRecord<'_>,
        right: &QueryRecord<'_>,
    ) -> Result<Ordering, AppError> {
        for sort in &self.sort {
            let left_value = value_for_field(left, &sort.field)?;
            let right_value = value_for_field(right, &sort.field)?;
            let ordering = compare_values(left_value.as_ref(), right_value.as_ref())?;
            if ordering != Ordering::Equal {
                return Ok(match sort.direction {
                    SortDirection::Asc => ordering,
                    SortDirection::Desc => ordering.reverse(),
                });
            }
        }
        Ok(Ordering::Equal)
    }
}

impl RecordQueryCondition {
    fn matches(&self, record: &QueryRecord<'_>) -> Result<bool, AppError> {
        let candidate = value_for_field(record, &self.field)?;
        match self.op {
            QueryOperator::Exists => {
                let should_exist = self.value.as_bool().unwrap_or(false);
                Ok(candidate.is_some() == should_exist)
            }
            QueryOperator::Eq => Ok(values_equal(candidate.as_ref(), &self.value)),
            QueryOperator::Ne => Ok(!values_equal(candidate.as_ref(), &self.value)),
            QueryOperator::In => {
                let Some(candidate) = candidate.as_ref() else {
                    return Ok(false);
                };
                Ok(self
                    .value
                    .as_array()
                    .map(|values| {
                        values
                            .iter()
                            .any(|value| values_equal(Some(candidate), value))
                    })
                    .unwrap_or(false))
            }
            QueryOperator::Contains => contains(candidate.as_ref(), &self.value),
            QueryOperator::Gt => compare_bool(candidate.as_ref(), &self.value, |ord| ord.is_gt()),
            QueryOperator::Gte => compare_bool(candidate.as_ref(), &self.value, |ord| ord.is_ge()),
            QueryOperator::Lt => compare_bool(candidate.as_ref(), &self.value, |ord| ord.is_lt()),
            QueryOperator::Lte => compare_bool(candidate.as_ref(), &self.value, |ord| ord.is_le()),
        }
    }
}

fn compare_bool(
    candidate: Option<&Value>,
    expected: &Value,
    predicate: impl Fn(Ordering) -> bool,
) -> Result<bool, AppError> {
    let Some(candidate) = candidate else {
        return Ok(false);
    };
    Ok(predicate(compare_values(Some(candidate), Some(expected))?))
}

fn compare_values(left: Option<&Value>, right: Option<&Value>) -> Result<Ordering, AppError> {
    match (left, right) {
        (None, None) => Ok(Ordering::Equal),
        (None, Some(_)) => Ok(Ordering::Less),
        (Some(_), None) => Ok(Ordering::Greater),
        (Some(Value::String(left)), Some(Value::String(right))) => Ok(left.cmp(right)),
        (Some(Value::Number(left)), Some(Value::Number(right))) => {
            let left = left
                .as_f64()
                .ok_or_else(|| AppError::BadRequest("cannot compare numeric value".to_string()))?;
            let right = right
                .as_f64()
                .ok_or_else(|| AppError::BadRequest("cannot compare numeric value".to_string()))?;
            left.partial_cmp(&right).ok_or_else(|| {
                AppError::BadRequest("numeric comparison returned no ordering".to_string())
            })
        }
        (Some(Value::Bool(left)), Some(Value::Bool(right))) => Ok(left.cmp(right)),
        _ => Err(AppError::BadRequest(
            "unsupported comparison between field value and query value".to_string(),
        )),
    }
}

fn contains(candidate: Option<&Value>, expected: &Value) -> Result<bool, AppError> {
    let Some(candidate) = candidate else {
        return Ok(false);
    };
    match candidate {
        Value::String(candidate) => Ok(expected
            .as_str()
            .map(|needle| candidate.contains(needle))
            .unwrap_or(false)),
        Value::Array(candidate) => Ok(candidate
            .iter()
            .any(|item| values_equal(Some(item), expected))),
        _ => Err(AppError::BadRequest(
            "operator `contains` only supports string or array fields".to_string(),
        )),
    }
}

fn values_equal(left: Option<&Value>, right: &Value) -> bool {
    match left {
        Some(left) => left == right,
        None => false,
    }
}

fn value_for_field(record: &QueryRecord<'_>, field: &str) -> Result<Option<Value>, AppError> {
    match field {
        "id" => Ok(Some(json!(record.id))),
        "model" => Ok(Some(json!(record.model))),
        "version" => Ok(Some(json!(record.version))),
        "created_at" => Ok(Some(json!(record.created_at.to_rfc3339()))),
        "updated_at" => Ok(Some(json!(record.updated_at.to_rfc3339()))),
        payload if payload.starts_with("payload.") => Ok(extract_payload_value(
            record.payload,
            &payload["payload.".len()..],
        )),
        "payload" => Ok(Some(record.payload.clone())),
        _ => Err(AppError::BadRequest(format!(
            "unsupported query field `{field}`"
        ))),
    }
}

fn extract_payload_value(payload: &Value, path: &str) -> Option<Value> {
    let mut current = payload;
    for segment in split_path(path) {
        match segment {
            PathSegment::Key(key) => current = current.get(&key)?,
            PathSegment::Index(index) => current = current.get(index)?,
        }
    }
    Some(current.clone())
}

#[derive(Debug)]
enum PathSegment {
    Key(String),
    Index(usize),
}

fn split_path(path: &str) -> Vec<PathSegment> {
    let mut segments = Vec::new();
    for raw_segment in path.split('.') {
        let mut remainder = raw_segment;
        loop {
            if let Some(start) = remainder.find('[') {
                if start > 0 {
                    segments.push(PathSegment::Key(remainder[..start].to_string()));
                }
                let end = remainder[start + 1..]
                    .find(']')
                    .map(|value| value + start + 1)
                    .unwrap_or(remainder.len());
                if let Ok(index) = remainder[start + 1..end].parse() {
                    segments.push(PathSegment::Index(index));
                }
                if end + 1 >= remainder.len() {
                    break;
                }
                remainder = &remainder[end + 1..];
            } else {
                if !remainder.is_empty() {
                    segments.push(PathSegment::Key(remainder.to_string()));
                }
                break;
            }
        }
    }
    segments
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_record() -> QueryRecord<'static> {
        static PAYLOAD: &str = r#"{"record_scope":"product","applied_schemas":[{"schema_url":"urn:test"}],"weight":12}"#;
        let payload: Value = serde_json::from_str(PAYLOAD).expect("payload json");
        let leaked = Box::leak(Box::new(payload));
        QueryRecord {
            id: "rec-1",
            model: "passport",
            version: "1.0.0",
            payload: leaked,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn payload_paths_support_dot_and_index_notation() {
        let value = value_for_field(&sample_record(), "payload.applied_schemas[0].schema_url")
            .expect("field lookup")
            .expect("field value");
        assert_eq!(value, json!("urn:test"));
    }

    #[test]
    fn contains_matches_arrays() {
        let condition = RecordQueryCondition {
            field: "payload.applied_schemas".to_string(),
            op: QueryOperator::Contains,
            value: json!({"schema_url":"urn:test"}),
        };
        assert!(
            condition
                .matches(&sample_record())
                .expect("query evaluation")
        );
    }
}

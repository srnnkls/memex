use super::*;
use crate::state::{FileIdentity, PendingToolCall};
use serde::de::DeserializeOwned;

#[derive(Deserialize)]
struct Extended<T> {
    #[serde(flatten)]
    _known: T,
    #[serde(flatten)]
    extra: Map<String, Value>,
}

fn extensions<T: DeserializeOwned>(value: Value) -> Result<Map<String, Value>> {
    Ok(serde_json::from_value::<Extended<T>>(value)?.extra)
}

fn preserve<T: DeserializeOwned + Serialize>(
    current: &T,
    previous: Option<&Value>,
) -> Result<Value> {
    let mut value = serde_json::to_value(current)?;
    if let Some(previous) = previous {
        let extra = extensions::<T>(previous.clone())?;
        value
            .as_object_mut()
            .context("checkpoint payload must be an object")?
            .extend(extra);
    }
    Ok(value)
}

pub(super) fn file_payload(file: &FileState, previous: Option<&str>) -> Result<String> {
    let old: Option<Value> = previous.map(serde_json::from_str).transpose()?;
    let mut value = preserve(file, old.as_ref())?;
    value["identity"] = preserve::<FileIdentity>(
        &file.identity,
        old.as_ref().and_then(|value| value.get("identity")),
    )?;
    let calls = value["pending_tool_calls"]
        .as_object_mut()
        .context("invalid pending tool calls")?;
    for (id, call) in &file.pending_tool_calls {
        let previous = old
            .as_ref()
            .and_then(|value| value.get("pending_tool_calls"))
            .and_then(|calls| calls.get(id));
        calls.insert(id.clone(), preserve::<PendingToolCall>(call, previous)?);
    }
    Ok(serde_json::to_string(&value)?)
}

pub(super) fn database_payload(
    databases: &HashMap<String, OpencodeDatabaseState>,
    previous: &str,
) -> Result<String> {
    let old: Value = serde_json::from_str(previous)?;
    let mut value = Map::new();
    for (path, database) in databases {
        value.insert(path.clone(), preserve(database, old.get(path))?);
    }
    Ok(serde_json::to_string(&value)?)
}

pub(super) fn legacy_extras(value: &Value) -> Result<String> {
    Ok(serde_json::to_string(&extensions::<IngestState>(
        value.clone(),
    )?)?)
}

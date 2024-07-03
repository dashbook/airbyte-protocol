use serde::{Deserialize, Serialize};

use crate::schema::JsonSchema;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE", tag = "type")]
pub enum AirbyteMessage {
    Catalog {
        catalog: AirbyteCatalog,
    },
    Log {
        log: AirbyteLogMessage,
    },
    Trace {
        trace: AirbyteTraceMessage,
    },
    Spec {
        spec: ConnectorSpecification,
    },
    Control {
        control: ConnectorSpecification,
    },
    ConnectionStatus {
        #[serde(rename = "connectionStatus")]
        connection_status: AirbyteConnectionStatus,
    },
    State {
        state: AirbyteStateMessage,
    },
    Record {
        record: AirbyteRecordMessage,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AirbyteRecordMessage {
    /// record data
    pub data: serde_json::Value,
    /// when the data was emitted from the source. epoch in millisecond.
    pub emitted_at: i64,
    /// Information about this record added mid-sync
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<AirbyteRecordMessageMeta>,
    /// namespace the data is associated with
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    /// stream the data is associated with
    pub stream: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AirbyteRecordMessageMeta {
    /// Lists of changes to the content of this record which occurred during syncing
    #[serde(skip_serializing_if = "Option::is_none")]
    pub changes: Option<Vec<AirbyteRecordMessageMetaChange>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AirbyteRecordMessageMetaChange {
    /// The type of change that occurred
    pub change: String,
    /// The field that had the change occur (required)
    pub field: String,
    /// The reason that the change occurred
    pub reason: String,
}

/// The type of state the other fields represent. Is set to LEGACY, the state data should be read
/// from the `data` field for backwards compatibility. If not set, assume the state object is type
/// LEGACY. GLOBAL means that the state should be read from `global` and means that it represents
/// the state for all the streams. It contains one shared state and individual stream states.
/// PER_STREAM means that the state should be read from `stream`. The state present in this field
/// correspond to the isolated state of the associated stream description.
///
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE", tag = "type")]
pub enum AirbyteStateMessage {
    Global {
        global: AirbyteGlobalState,
        #[serde(rename = "destinationStats", skip_serializing_if = "Option::is_none")]
        destination_stats: Option<AirbyteStateStats>,
        #[serde(rename = "sourceStats", skip_serializing_if = "Option::is_none")]
        source_stats: Option<AirbyteStateStats>,
    },
    Stream {
        stream: AirbyteStreamState,
        #[serde(rename = "destinationStats", skip_serializing_if = "Option::is_none")]
        destination_stats: Option<AirbyteStateStats>,
        #[serde(rename = "sourceStats", skip_serializing_if = "Option::is_none")]
        source_stats: Option<AirbyteStateStats>,
    },
    Legacy {
        data: serde_json::Value,
        #[serde(rename = "destinationStats", skip_serializing_if = "Option::is_none")]
        destination_stats: Option<AirbyteStateStats>,
        #[serde(rename = "sourceStats", skip_serializing_if = "Option::is_none")]
        source_stats: Option<AirbyteStateStats>,
    },
    #[serde(untagged)]
    Empty {
        data: serde_json::Value,
        #[serde(rename = "destinationStats", skip_serializing_if = "Option::is_none")]
        destination_stats: Option<AirbyteStateStats>,
        #[serde(rename = "sourceStats", skip_serializing_if = "Option::is_none")]
        source_stats: Option<AirbyteStateStats>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AirbyteGlobalState {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shared_state: Option<serde_json::Value>,
    pub stream_states: Vec<AirbyteStreamState>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AirbyteStreamState {
    pub stream_descriptor: StreamDescriptor,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream_state: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StreamDescriptor {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

impl StreamDescriptor {
    pub fn new(name: &str, namespace: Option<&str>) -> Self {
        Self {
            name: name.to_owned(),
            namespace: namespace.map(ToOwned::to_owned),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AirbyteStateStats {
    /// the number of records which were emitted for this state message, for this stream or global.
    /// While the value should always be a round number, it is defined as a double to account for
    /// integer overflows, and the value should always have a decimal point for proper
    /// serialization.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "recordCount")]
    pub record_count: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
/// Airbyte stream schema catalog
pub struct AirbyteCatalog {
    pub streams: Vec<AirbyteStream>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AirbyteStream {
    /// Stream's name.
    pub name: String,
    /// Stream schema using Json Schema specs.
    pub json_schema: JsonSchema,
    /// Path to the field that will be used to determine if a record is new or modified since the
    /// last sync. If not provided by the source, the end user will have to specify the comparable
    /// themselves.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_cursor_field: Option<Vec<String>>,
    /// If the stream is resumable or not. Should be set to true if the stream supports
    /// incremental. Defaults to false.
    /// Primarily used by the Platform in Full Refresh to determine if a Full Refresh stream should
    /// actually be treated as incremental within a job.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_resumable: Option<bool>,
    /// Optional Source-defined namespace. Currently only used by JDBC destinations to determine
    /// what schema to write to. Airbyte streams from the same sources should have the same
    /// namespace.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    /// If the source defines the cursor field, then any other cursor field inputs will be ignored.
    /// If it does not,
    /// either the user_provided one is used, or the default one is used as a backup. This field
    /// must be set if
    /// is_resumable is set to true, including resumable full refresh synthetic cursors.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_defined_cursor: Option<bool>,
    /// If the source defines the primary key, paths to the fields that will be used as a primary
    /// key. If not provided by the source, the end user will have to specify the primary key
    /// themselves.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_defined_primary_key: Option<Vec<Vec<String>>>,
    /// List of sync modes supported by this stream.
    pub supported_sync_modes: Vec<SyncMode>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
/// Airbyte stream schema catalog
pub struct ConfiguredAirbyteCatalog {
    pub streams: Vec<ConfiguredAirbyteStream>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ConfiguredAirbyteStream {
    /// Path to the field that will be used to determine if a record is new or modified since the
    /// last sync. This field is REQUIRED if `sync_mode` is `incremental`. Otherwise it is ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor_field: Option<Vec<String>>,
    pub destination_sync_mode: DestinationSyncMode,
    /// Monotically increasing numeric id representing the current generation of a stream. This id
    /// can be shared across syncs.
    /// If this is null, it means that the platform is not supporting the refresh and it is
    /// expected that no extra id will be added to the records and no data from previous generation
    /// will be cleanup.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub generation_id: Option<i64>,
    /// The minimum generation id which is needed in a stream. If it is present, the destination
    /// will try to delete the data that are part of a generation lower than this property. If the
    /// minimum generation is equals to 0, no data deletion is expected from the destiantion
    /// If this is null, it means that the platform is not supporting the refresh and it is
    /// expected that no extra id will be added to the records and no data from previous generation
    /// will be cleanup.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub minimum_generation_id: Option<i64>,
    /// Paths to the fields that will be used as primary key. This field is REQUIRED if
    /// `destination_sync_mode` is `*_dedup`. Otherwise it is ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub primary_key: Option<Vec<Vec<String>>>,
    pub stream: AirbyteStream,
    /// Monotically increasing numeric id representing the current sync id. This is aimed to be
    /// unique per sync.
    /// If this is null, it means that the platform is not supporting the refresh and it is
    /// expected that no extra id will be added to the records and no data from previous generation
    /// will be cleanup.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sync_id: Option<i64>,
    pub sync_mode: SyncMode,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
/// Airbyte connection status
pub struct AirbyteConnectionStatus {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE", tag = "type")]
pub enum AirbyteControlMessage {
    ConnectorConfig {
        connector_config: AirbyteControlConnectorConfigMessage,
        emitted_at: f64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AirbyteControlConnectorConfigMessage {
    /// the config items from this connector's spec to update
    pub config: serde_json::Value,
}

/// Specification of a connector (source/destination)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ConnectorSpecification {
    /// Additional and optional specification object to describe what an 'advanced' Auth flow would
    /// need to function.
    ///   - A connector should be able to fully function with the configuration as described by the
    /// ConnectorSpecification in a 'basic' mode.
    ///   - The 'advanced' mode provides easier UX for the user with UI improvements and
    /// automations. However, this requires further setup on the
    ///   server side by instance or workspace admins beforehand. The trade-off is that the user
    /// does not have to provide as many technical
    ///   inputs anymore and the auth process is faster and easier to complete.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub advanced_auth: Option<ConnectorSpecificationAdvancedAuth>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "changelogUrl")]
    pub changelog_url: Option<String>,
    /// ConnectorDefinition specific blob. Must be a valid JSON string.
    #[serde(rename = "connectionSpecification")]
    pub connection_specification: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "documentationUrl")]
    pub documentation_url: Option<String>,
    /// the Airbyte Protocol version supported by the connector. Protocol versioning uses SemVer.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protocol_version: Option<String>,
    /// List of destination sync modes supported by the connector
    #[serde(skip_serializing_if = "Option::is_none")]
    pub supported_destination_sync_modes: Option<Vec<DestinationSyncMode>>,
    /// If the connector supports DBT or not.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "supportsDBT")]
    pub supports_dbt: Option<bool>,
    /// (deprecated) If the connector supports incremental mode or not.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "supportsIncremental")]
    pub supports_incremental: Option<bool>,
    /// If the connector supports normalization or not.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "supportsNormalization")]
    pub supports_normalization: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ConnectorSpecificationAdvancedAuth {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_flow_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oauth_config_specification: Option<OauthConfigSpecification>,
    /// Json Path to a field in the connectorSpecification that should exist for the advanced auth
    /// to be applicable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub predicate_key: Option<Vec<String>>,
    /// Value of the predicate_key fields for the advanced auth to be applicable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub predicate_value: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename = "OAuthConfigSpecification")]
pub struct OauthConfigSpecification {
    /// OAuth specific blob. This is a Json Schema used to validate Json configurations produced by
    /// the OAuth flows as they are
    /// returned by the distant OAuth APIs.
    /// Must be a valid JSON describing the fields to merge back to
    /// `ConnectorSpecification.connectionSpecification`.
    /// For each field, a special annotation `path_in_connector_config` can be specified to
    /// determine where to merge it,
    ///
    #[serde(skip_serializing_if = "Option::is_none")]
    pub complete_oauth_output_specification: Option<serde_json::Value>,
    /// OAuth specific blob. This is a Json Schema used to validate Json configurations persisted
    /// as Airbyte Server configurations.
    /// Must be a valid non-nested JSON describing additional fields configured by the Airbyte
    /// Instance or Workspace Admins to be used by the
    /// server when completing an OAuth flow (typically exchanging an auth code for refresh token).
    ///
    #[serde(skip_serializing_if = "Option::is_none")]
    pub complete_oauth_server_input_specification: Option<serde_json::Value>,
    /// OAuth specific blob. This is a Json Schema used to validate Json configurations persisted
    /// as Airbyte Server configurations that
    /// also need to be merged back into the connector configuration at runtime.
    /// This is a subset configuration of `complete_oauth_server_input_specification` that filters
    /// fields out to retain only the ones that
    /// are necessary for the connector to function with OAuth. (some fields could be used during
    /// oauth flows but not needed afterwards, therefore
    /// they would be listed in the `complete_oauth_server_input_specification` but not
    /// `complete_oauth_server_output_specification`)
    /// Must be a valid non-nested JSON describing additional fields configured by the Airbyte
    /// Instance or Workspace Admins to be used by the
    /// connector when using OAuth flow APIs.
    /// These fields are to be merged back to `ConnectorSpecification.connectionSpecification`.
    /// For each field, a special annotation `path_in_connector_config` can be specified to
    /// determine where to merge it,
    ///
    #[serde(skip_serializing_if = "Option::is_none")]
    pub complete_oauth_server_output_specification: Option<serde_json::Value>,
    /// OAuth specific blob. This is a Json Schema used to validate Json configurations used as
    /// input to OAuth.
    /// Must be a valid non-nested JSON that refers to properties from
    /// ConnectorSpecification.connectionSpecification
    /// using special annotation 'path_in_connector_config'.
    /// These are input values the user is entering through the UI to authenticate to the
    /// connector, that might also shared
    /// as inputs for syncing data via the connector.
    ///
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oauth_user_input_from_connector_config_specification: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum DestinationSyncMode {
    #[serde(rename = "append")]
    Append,
    #[serde(rename = "overwrite")]
    Overwrite,
    #[serde(rename = "append_dedup")]
    AppendDedup,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AirbyteLogMessage {
    /// log level
    pub level: String,
    /// log message
    pub message: String,
    /// an optional stack trace if the log message corresponds to an exception
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stack_trace: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub enum SyncMode {
    #[default]
    #[serde(rename = "full_refresh")]
    FullRefresh,
    #[serde(rename = "incremental")]
    Incremental,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE", tag = "type")]
pub enum AirbyteTraceMessage {
    Analytics {
        analytics: AirbyteAnalyticsTraceMessage,
        emited_at: f64,
    },
    Error {
        error: AirbyteErrorTraceMessage,
        emitted_at: f64,
    },
    Estimate {
        estimate: AirbyteEstimateTraceMessage,
        emitted_at: f64,
    },
    StreamStatus {
        stream_status: AirbyteStreamStatusTraceMessage,
        emitted_at: f64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AirbyteStreamStatusTraceMessage {
    /// The reasons associated with the status of the stream
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasons: Option<Vec<AirbyteStreamStatusReason>>,
    /// The current status of the stream
    pub status: AirbyteStreamStatus,
    /// The stream associated with the status
    pub stream_descriptor: StreamDescriptor,
}

/// The current status of a stream within the context of an executing synchronization job.
///
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "UPPERCASE")]
pub enum AirbyteStreamStatus {
    Started,
    Running,
    Complete,
    Incomplete,
}

/// The reason associated with the status of the stream.
///
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AirbyteStreamStatusReason {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_limited: Option<AirbyteStreamStatusRateLimitedReason>,
    #[serde(rename = "type")]
    pub r#type: AirbyteStreamStatusReasonType,
}

/// Rate Limited Information
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AirbyteStreamStatusRateLimitedReason {
    /// Optional time in ms representing when the API quota is going to be reset
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quota_reset: Option<i64>,
}

/// Type of reason
///
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum AirbyteStreamStatusReasonType {
    #[serde(rename = "RATE_LIMITED")]
    RateLimited,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AirbyteEstimateTraceMessage {
    /// The estimated number of bytes to be emitted by this sync for this stream
    #[serde(skip_serializing_if = "Option::is_none")]
    pub byte_estimate: Option<i64>,
    /// The name of the stream
    pub name: String,
    /// The namespace of the stream
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    /// The estimated number of rows to be emitted by this sync for this stream
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row_estimate: Option<i64>,
}

/// A message to communicate usage information about the connector which is not captured by regular
/// sync analytics because it's specific to the connector internals.
/// This is useful to understand how the connector is used and how to improve it. Each message is
/// an event with a type and an optional payload value (both of them being strings). The event
/// types should not be dynamically generated but defined statically. The payload value is optional
/// and can contain arbitrary strings.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AirbyteAnalyticsTraceMessage {
    /// The value of the event - can be an arbitrary string. In case the value is numeric, it
    /// should be converted to a string. Casting for analytics purposes can happen in the
    /// warehouse.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AirbyteErrorTraceMessage {
    /// The type of error
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_type: Option<String>,
    /// The internal error that caused the failure
    #[serde(skip_serializing_if = "Option::is_none")]
    pub internal_message: Option<String>,
    /// A user-friendly message that indicates the cause of the error
    pub message: String,
    /// The full stack trace of the error
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stack_trace: Option<String>,
    /// The stream associated with the error, if known (optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream_descriptor: Option<StreamDescriptor>,
}

#[cfg(test)]
pub mod tests {

    use serde_json::json;

    use crate::message::{AirbyteMessage, AirbyteRecordMessage};

    #[test]
    fn test_record() {
        let input = r#"{"type": "RECORD", "record": { "stream": "users", "data": {"id": 1, "name": "Chris"}, "emitted_at": 1}}"#;

        let record: AirbyteMessage = serde_json::from_str(input).unwrap();

        let expected = AirbyteMessage::Record {
            record: AirbyteRecordMessage {
                stream: "users".to_string(),
                data: json!({"id": 1, "name": "Chris"}),
                emitted_at: 1,
                meta: None,
                namespace: None,
            },
        };
        assert_eq!(record, expected);
    }
}

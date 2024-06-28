use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JsonSchema {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(flatten)]
    pub r#type: Type,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum Type {
    PrimitiveFormat {
        r#type: PrimitiveFormat,
        format: Format,
    },
    SingleFormat {
        r#type: [PrimitiveFormat; 1],
        format: Format,
    },
    VariantFormat {
        r#type: [PrimitiveFormat; 2],
        format: Format,
    },
    Primitive {
        r#type: Primitive,
    },
    Single {
        r#type: [Primitive; 1],
    },
    Variant {
        r#type: [Primitive; 2],
    },
    Compound(Compound),
    Empty(Empty),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Primitive {
    Null,
    Boolean,
    Integer,
    Number,
    String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum PrimitiveFormat {
    Null,
    String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum Format {
    Date,
    Time,
    DateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum Compound {
    Array(Array),
    Object(Object),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Array {
    pub items: Box<Type>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Object {
    pub properties: HashMap<String, Type>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub required: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub additional_properties: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Empty {}

#[cfg(test)]
pub mod tests {
    use std::collections::HashMap;

    use crate::schema::{
        Array, Compound, Empty, Format, JsonSchema, Object, Primitive, PrimitiveFormat, Type,
    };

    #[test]
    fn test_null() {
        let input = r#"{"type": "null"}"#;

        let value: JsonSchema = serde_json::from_str(input).unwrap();

        let expected = JsonSchema {
            title: None,
            description: None,
            r#type: Type::Primitive {
                r#type: Primitive::Null,
            },
        };
        assert_eq!(value, expected);
    }

    #[test]
    fn test_boolean() {
        let input = r#"{"type": "boolean"}"#;

        let value: JsonSchema = serde_json::from_str(input).unwrap();

        let expected = JsonSchema {
            title: None,
            description: None,
            r#type: Type::Primitive {
                r#type: Primitive::Boolean,
            },
        };
        assert_eq!(value, expected);
    }

    #[test]
    fn test_integer() {
        let input = r#"{"type": "integer"}"#;

        let value: JsonSchema = serde_json::from_str(input).unwrap();

        let expected = JsonSchema {
            title: None,
            description: None,
            r#type: Type::Primitive {
                r#type: Primitive::Integer,
            },
        };
        assert_eq!(value, expected);
    }

    #[test]
    fn test_string() {
        let input = r#"{"type": "string"}"#;

        let value: JsonSchema = serde_json::from_str(input).unwrap();

        let expected = JsonSchema {
            title: None,
            description: None,
            r#type: Type::Primitive {
                r#type: Primitive::String,
            },
        };
        assert_eq!(value, expected);
    }

    #[test]
    fn test_single() {
        let input = r#"{"type": ["string"]}"#;

        let value: JsonSchema = serde_json::from_str(input).unwrap();

        let expected = JsonSchema {
            title: None,
            description: None,
            r#type: Type::Single {
                r#type: [Primitive::String; 1],
            },
        };
        assert_eq!(value, expected);
    }

    #[test]
    fn test_array() {
        let input = r#"{"type": "array", "items": {"type": "integer"}}"#;

        let value: JsonSchema = serde_json::from_str(input).unwrap();

        let expected = JsonSchema {
            title: None,
            description: None,
            r#type: Type::Compound(Compound::Array(Array {
                items: Box::new(Type::Primitive {
                    r#type: Primitive::Integer,
                }),
            })),
        };
        assert_eq!(value, expected);
    }

    #[test]
    fn test_object() {
        let input =
            r#"{"required": ["id"], "type": "object", "properties": {"id": {"type": "integer"}}}"#;

        let schema: JsonSchema = serde_json::from_str(input).unwrap();

        let expected = JsonSchema {
            title: None,
            description: None,
            r#type: Type::Compound(Compound::Object(Object {
                properties: HashMap::from_iter(vec![(
                    "id".to_string(),
                    Type::Primitive {
                        r#type: Primitive::Integer,
                    },
                )]),
                required: Some(vec!["id".to_string()]),
                additional_properties: None,
            })),
        };
        assert_eq!(schema, expected);
    }

    #[test]
    fn test_variant() {
        let input = r#"{"required": ["id"], "type": "object", "properties": {"id": {"type": ["integer", "null"]}}}"#;

        let schema: JsonSchema = serde_json::from_str(input).unwrap();

        let expected = JsonSchema {
            title: None,
            description: None,
            r#type: Type::Compound(Compound::Object(Object {
                properties: HashMap::from_iter(vec![(
                    "id".to_string(),
                    Type::Variant {
                        r#type: [Primitive::Integer, Primitive::Null],
                    },
                )]),
                required: Some(vec!["id".to_string()]),
                additional_properties: None,
            })),
        };
        assert_eq!(schema, expected);
    }

    #[test]
    fn test_date() {
        let input = r#"
        {
        "type": "object",
        "properties": {
          "order_date": {
            "type": [
              "null",
              "string"
            ],
            "format": "date-time"
          }
        }}
       "#;

        let schema: JsonSchema = serde_json::from_str(input).unwrap();

        let expected = JsonSchema {
            title: None,
            description: None,
            r#type: Type::Compound(Compound::Object(Object {
                properties: HashMap::from_iter(vec![(
                    "order_date".to_string(),
                    Type::VariantFormat {
                        r#type: [PrimitiveFormat::Null, PrimitiveFormat::String],
                        format: Format::DateTime,
                    },
                )]),
                required: None,
                additional_properties: None,
            })),
        };
        assert_eq!(schema, expected);
    }

    #[test]
    fn test_empty() {
        let input = r#"{"required": ["id"], "type": "object", "properties": {"id": {}}}"#;

        let schema: JsonSchema = serde_json::from_str(input).unwrap();

        let expected = JsonSchema {
            title: None,
            description: None,
            r#type: Type::Compound(Compound::Object(Object {
                properties: HashMap::from_iter(vec![("id".to_string(), Type::Empty(Empty {}))]),
                required: Some(vec!["id".to_string()]),
                additional_properties: None,
            })),
        };
        assert_eq!(schema, expected);
    }
}

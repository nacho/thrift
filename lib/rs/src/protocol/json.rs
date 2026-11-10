// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! The Thrift JSON protocol, wire-compatible with `TJSONProtocol` in the
//! C++, Java, Python and Go libraries.
//!
//! Thrift types are encoded as follows:
//!
//! * integers are JSON numbers and booleans are the numbers `1` and `0`
//! * doubles are JSON numbers, except NaN and the infinities, which are the
//!   strings `"NaN"`, `"Infinity"` and `"-Infinity"`
//! * strings are JSON strings, binary values are base64 JSON strings and
//!   UUIDs are JSON strings in the hyphenated form
//! * structs are JSON objects keyed by field id, where each value is an
//!   object with a single member keyed by the type tag of the field:
//!   `{"1":{"i32":5},"2":{"str":"x"}}`
//! * lists and sets are `["<element tag>",<size>,<elements>...]`
//! * maps are `["<key tag>","<value tag>",<size>,{<key>:<value>,...}]`
//! * messages are `[1,"<name>",<type>,<sequence number>,<struct>]`
//!
//! Numbers used as object keys (field ids and numeric map keys) are quoted.

use std::convert::TryFrom;
use std::str::FromStr;

use super::{
    TFieldIdentifier, TInputProtocol, TInputProtocolFactory, TListIdentifier, TMapIdentifier,
    TMessageIdentifier, TMessageType, TOutputProtocol, TOutputProtocolFactory, TSetIdentifier,
    TStructIdentifier, TType,
};
use crate::transport::{TReadTransport, TWriteTransport};
use crate::{ProtocolErrorKind, TConfiguration, TransportErrorKind, new_protocol_error};

use base64::Engine;
use base64::alphabet;
use base64::engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig};

const THRIFT_JSON_VERSION: i64 = 1;

const BASE64: GeneralPurpose = GeneralPurpose::new(
    &alphabet::STANDARD,
    GeneralPurposeConfig::new()
        .with_encode_padding(false)
        .with_decode_allow_trailing_bits(true)
        .with_decode_padding_mode(DecodePaddingMode::RequireNone),
);

fn type_to_tag(field_type: TType) -> crate::Result<&'static str> {
    match field_type {
        TType::Bool => Ok("tf"),
        TType::I08 => Ok("i8"),
        TType::I16 => Ok("i16"),
        TType::I32 => Ok("i32"),
        TType::I64 => Ok("i64"),
        TType::Double => Ok("dbl"),
        TType::String | TType::Utf7 => Ok("str"),
        TType::Struct => Ok("rec"),
        TType::Map => Ok("map"),
        TType::Set => Ok("set"),
        TType::List => Ok("lst"),
        TType::Uuid => Ok("uid"),
        TType::Stop | TType::Void => Err(new_protocol_error(
            ProtocolErrorKind::NotImplemented,
            format!("type {} has no JSON type tag", field_type),
        )),
    }
}

fn tag_to_type(tag: &[u8]) -> crate::Result<TType> {
    match tag {
        b"tf" => Ok(TType::Bool),
        b"i8" => Ok(TType::I08),
        b"i16" => Ok(TType::I16),
        b"i32" => Ok(TType::I32),
        b"i64" => Ok(TType::I64),
        b"dbl" => Ok(TType::Double),
        b"str" => Ok(TType::String),
        b"rec" => Ok(TType::Struct),
        b"map" => Ok(TType::Map),
        b"set" => Ok(TType::Set),
        b"lst" => Ok(TType::List),
        b"uid" => Ok(TType::Uuid),
        _ => Err(new_protocol_error(
            ProtocolErrorKind::NotImplemented,
            format!("Unrecognized type: {:?}", String::from_utf8_lossy(tag)),
        )),
    }
}

fn invalid_data<S: Into<String>>(message: S) -> crate::Error {
    new_protocol_error(ProtocolErrorKind::InvalidData, message)
}

fn is_json_numeric(b: u8) -> bool {
    matches!(b, b'+' | b'-' | b'.' | b'0'..=b'9' | b'E' | b'e')
}

fn hex_value(b: u8) -> crate::Result<u16> {
    match b {
        b'0'..=b'9' => Ok((b - b'0').into()),
        b'a'..=b'f' => Ok((b - b'a' + 10).into()),
        b'A'..=b'F' => Ok((b - b'A' + 10).into()),
        _ => Err(invalid_data(format!(
            "Expected hex digit, got {:?}",
            char::from(b)
        ))),
    }
}

fn is_high_surrogate(code_unit: u16) -> bool {
    (0xD800..=0xDBFF).contains(&code_unit)
}

fn is_low_surrogate(code_unit: u16) -> bool {
    (0xDC00..=0xDFFF).contains(&code_unit)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum JsonContext {
    Base,
    List { first: bool },
    Pair { first: bool, colon: bool },
}

impl JsonContext {
    fn list() -> Self {
        JsonContext::List { first: true }
    }

    fn pair() -> Self {
        JsonContext::Pair {
            first: true,
            colon: true,
        }
    }

    fn next_separator(&mut self) -> Option<u8> {
        match self {
            JsonContext::Base => None,
            JsonContext::List { first } => {
                if *first {
                    *first = false;
                    None
                } else {
                    Some(b',')
                }
            }
            JsonContext::Pair { first, colon } => {
                if *first {
                    *first = false;
                    *colon = true;
                    None
                } else {
                    let separator = if *colon { b':' } else { b',' };
                    *colon = !*colon;
                    Some(separator)
                }
            }
        }
    }

    fn escape_num(&self) -> bool {
        matches!(self, JsonContext::Pair { colon: true, .. })
    }
}

#[derive(Debug)]
struct JsonContextStack {
    current: JsonContext,
    parents: Vec<JsonContext>,
}

impl JsonContextStack {
    fn new() -> Self {
        JsonContextStack {
            current: JsonContext::Base,
            parents: Vec::new(),
        }
    }

    fn push(&mut self, context: JsonContext) {
        self.parents.push(self.current);
        self.current = context;
    }

    fn pop(&mut self) {
        self.current = self.parents.pop().unwrap_or(JsonContext::Base);
    }

    fn reset(&mut self) {
        self.current = JsonContext::Base;
        self.parents.clear();
    }
}

fn escape_json_byte(out: &mut Vec<u8>, b: u8) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    match b {
        b'"' => out.extend_from_slice(b"\\\""),
        b'\\' => out.extend_from_slice(b"\\\\"),
        0x08 => out.extend_from_slice(b"\\b"),
        0x09 => out.extend_from_slice(b"\\t"),
        0x0A => out.extend_from_slice(b"\\n"),
        0x0C => out.extend_from_slice(b"\\f"),
        0x0D => out.extend_from_slice(b"\\r"),
        0x00..=0x1F => {
            out.extend_from_slice(b"\\u00");
            out.push(HEX[(b >> 4) as usize]);
            out.push(HEX[(b & 0x0F) as usize]);
        }
        _ => out.push(b),
    }
}

fn format_double(d: f64) -> String {
    let magnitude = d.abs();
    if d == 0.0 || (1e-5..1e16).contains(&magnitude) {
        format!("{}", d)
    } else {
        format!("{:e}", d)
    }
}

/// Write messages using the Thrift JSON protocol.
///
/// The output is the same as that of `TJSONProtocol` in the other Thrift
/// libraries, without any whitespace.
///
/// A struct can be written on its own, without a message around it, which
/// is useful to exchange single values with other JSON protocol users.
///
/// # Examples
///
/// ```
/// use thrift::protocol::{TFieldIdentifier, TJsonOutputProtocol, TOutputProtocol};
/// use thrift::protocol::{TStructIdentifier, TType};
///
/// let mut buf = Vec::new();
/// let mut protocol = TJsonOutputProtocol::new(&mut buf);
///
/// protocol.write_struct_begin(&TStructIdentifier::new("Point")).unwrap();
/// protocol.write_field_begin(&TFieldIdentifier::new("x", TType::I32, 1)).unwrap();
/// protocol.write_i32(5).unwrap();
/// protocol.write_field_end().unwrap();
/// protocol.write_field_stop().unwrap();
/// protocol.write_struct_end().unwrap();
///
/// assert_eq!(buf, br#"{"1":{"i32":5}}"#);
/// ```
#[derive(Debug)]
pub struct TJsonOutputProtocol<T>
where
    T: TWriteTransport,
{
    transport: T,
    context: JsonContextStack,
    config: TConfiguration,
    recursion_depth: usize,
}

impl<T> TJsonOutputProtocol<T>
where
    T: TWriteTransport,
{
    /// Create a `TJsonOutputProtocol` that writes bytes to `transport`.
    pub fn new(transport: T) -> Self {
        Self::with_config(transport, TConfiguration::default())
    }

    /// Create a `TJsonOutputProtocol` with a custom configuration.
    pub fn with_config(transport: T, config: TConfiguration) -> Self {
        TJsonOutputProtocol {
            transport,
            context: JsonContextStack::new(),
            config,
            recursion_depth: 0,
        }
    }

    fn check_recursion_depth(&self) -> crate::Result<()> {
        if let Some(limit) = self.config.max_recursion_depth() {
            if self.recursion_depth >= limit {
                return Err(new_protocol_error(
                    ProtocolErrorKind::DepthLimit,
                    format!("Maximum recursion depth {} exceeded", limit),
                ));
            }
        }
        Ok(())
    }

    fn write_context(&mut self) -> crate::Result<()> {
        if let Some(separator) = self.context.current.next_separator() {
            self.transport.write_all(&[separator])?;
        }
        Ok(())
    }

    fn write_json_string(&mut self, s: &[u8]) -> crate::Result<()> {
        self.write_context()?;
        let mut out = Vec::with_capacity(s.len() + 2);
        out.push(b'"');
        for &b in s {
            escape_json_byte(&mut out, b);
        }
        out.push(b'"');
        self.transport.write_all(&out).map_err(From::from)
    }

    fn write_json_number(&mut self, number: &str, quoted: bool) -> crate::Result<()> {
        if quoted {
            self.transport.write_all(b"\"")?;
            self.transport.write_all(number.as_bytes())?;
            self.transport.write_all(b"\"").map_err(From::from)
        } else {
            self.transport
                .write_all(number.as_bytes())
                .map_err(From::from)
        }
    }

    fn write_json_integer(&mut self, i: i64) -> crate::Result<()> {
        self.write_context()?;
        let quoted = self.context.current.escape_num();
        self.write_json_number(&i.to_string(), quoted)
    }

    fn write_json_double(&mut self, d: f64) -> crate::Result<()> {
        self.write_context()?;
        let (number, special) = if d.is_nan() {
            ("NaN".to_owned(), true)
        } else if d.is_infinite() {
            let number = if d > 0.0 { "Infinity" } else { "-Infinity" };
            (number.to_owned(), true)
        } else {
            (format_double(d), false)
        };
        let quoted = special || self.context.current.escape_num();
        self.write_json_number(&number, quoted)
    }

    fn write_json_base64(&mut self, b: &[u8]) -> crate::Result<()> {
        self.write_context()?;
        let mut out = Vec::with_capacity(b.len().div_ceil(3) * 4 + 2);
        out.push(b'"');
        out.extend_from_slice(BASE64.encode(b).as_bytes());
        out.push(b'"');
        self.transport.write_all(&out).map_err(From::from)
    }

    fn write_json_object_start(&mut self) -> crate::Result<()> {
        self.write_context()?;
        self.transport.write_all(b"{")?;
        self.context.push(JsonContext::pair());
        Ok(())
    }

    fn write_json_object_end(&mut self) -> crate::Result<()> {
        self.context.pop();
        self.transport.write_all(b"}").map_err(From::from)
    }

    fn write_json_array_start(&mut self) -> crate::Result<()> {
        self.write_context()?;
        self.transport.write_all(b"[")?;
        self.context.push(JsonContext::list());
        Ok(())
    }

    fn write_json_array_end(&mut self) -> crate::Result<()> {
        self.context.pop();
        self.transport.write_all(b"]").map_err(From::from)
    }

    fn write_type_tag(&mut self, field_type: TType) -> crate::Result<()> {
        let tag = type_to_tag(field_type)?;
        self.write_json_string(tag.as_bytes())
    }
}

impl<T> TOutputProtocol for TJsonOutputProtocol<T>
where
    T: TWriteTransport,
{
    fn write_message_begin(&mut self, identifier: &TMessageIdentifier) -> crate::Result<()> {
        self.context.reset();
        self.write_json_array_start()?;
        self.write_json_integer(THRIFT_JSON_VERSION)?;
        self.write_json_string(identifier.name.as_bytes())?;
        self.write_json_integer(u8::from(identifier.message_type).into())?;
        self.write_json_integer(identifier.sequence_number.into())
    }

    fn write_message_end(&mut self) -> crate::Result<()> {
        self.write_json_array_end()
    }

    fn write_struct_begin(&mut self, _: &TStructIdentifier) -> crate::Result<()> {
        self.check_recursion_depth()?;
        self.write_json_object_start()?;
        self.recursion_depth += 1;
        Ok(())
    }

    fn write_struct_end(&mut self) -> crate::Result<()> {
        self.write_json_object_end()?;
        self.recursion_depth = self.recursion_depth.saturating_sub(1);
        Ok(())
    }

    fn write_field_begin(&mut self, identifier: &TFieldIdentifier) -> crate::Result<()> {
        let id = identifier.id.ok_or_else(|| {
            new_protocol_error(
                ProtocolErrorKind::Unknown,
                format!(
                    "cannot write identifier {:?} without sequence number",
                    identifier
                ),
            )
        })?;
        self.write_json_integer(id.into())?;
        self.write_json_object_start()?;
        self.write_type_tag(identifier.field_type)
    }

    fn write_field_end(&mut self) -> crate::Result<()> {
        self.write_json_object_end()
    }

    fn write_field_stop(&mut self) -> crate::Result<()> {
        Ok(())
    }

    fn write_bool(&mut self, b: bool) -> crate::Result<()> {
        self.write_json_integer(b.into())
    }

    fn write_bytes(&mut self, b: &[u8]) -> crate::Result<()> {
        self.write_json_base64(b)
    }

    fn write_i8(&mut self, i: i8) -> crate::Result<()> {
        self.write_json_integer(i.into())
    }

    fn write_i16(&mut self, i: i16) -> crate::Result<()> {
        self.write_json_integer(i.into())
    }

    fn write_i32(&mut self, i: i32) -> crate::Result<()> {
        self.write_json_integer(i.into())
    }

    fn write_i64(&mut self, i: i64) -> crate::Result<()> {
        self.write_json_integer(i)
    }

    fn write_double(&mut self, d: f64) -> crate::Result<()> {
        self.write_json_double(d)
    }

    fn write_uuid(&mut self, uuid: &uuid::Uuid) -> crate::Result<()> {
        let mut buf = uuid::Uuid::encode_buffer();
        let s = uuid.hyphenated().encode_lower(&mut buf);
        self.write_json_string(s.as_bytes())
    }

    fn write_string(&mut self, s: &str) -> crate::Result<()> {
        self.write_json_string(s.as_bytes())
    }

    fn write_list_begin(&mut self, identifier: &TListIdentifier) -> crate::Result<()> {
        self.write_json_array_start()?;
        self.write_type_tag(identifier.element_type)?;
        self.write_json_integer(identifier.size.into())
    }

    fn write_list_end(&mut self) -> crate::Result<()> {
        self.write_json_array_end()
    }

    fn write_set_begin(&mut self, identifier: &TSetIdentifier) -> crate::Result<()> {
        self.write_json_array_start()?;
        self.write_type_tag(identifier.element_type)?;
        self.write_json_integer(identifier.size.into())
    }

    fn write_set_end(&mut self) -> crate::Result<()> {
        self.write_json_array_end()
    }

    fn write_map_begin(&mut self, identifier: &TMapIdentifier) -> crate::Result<()> {
        let (key_type, value_type) = match (identifier.key_type, identifier.value_type) {
            (Some(key_type), Some(value_type)) => (key_type, value_type),
            _ => {
                return Err(new_protocol_error(
                    ProtocolErrorKind::InvalidData,
                    format!(
                        "map identifier to write should contain key and value types: {:?}",
                        identifier
                    ),
                ));
            }
        };
        self.write_json_array_start()?;
        self.write_type_tag(key_type)?;
        self.write_type_tag(value_type)?;
        self.write_json_integer(identifier.size.into())?;
        self.write_json_object_start()
    }

    fn write_map_end(&mut self) -> crate::Result<()> {
        self.write_json_object_end()?;
        self.write_json_array_end()
    }

    fn flush(&mut self) -> crate::Result<()> {
        self.transport.flush().map_err(From::from)
    }

    fn write_byte(&mut self, b: u8) -> crate::Result<()> {
        self.write_json_integer(b.into())
    }
}

/// Factory for creating instances of `TJsonOutputProtocol`.
#[derive(Default)]
pub struct TJsonOutputProtocolFactory;

impl TJsonOutputProtocolFactory {
    /// Create a `TJsonOutputProtocolFactory`.
    pub fn new() -> TJsonOutputProtocolFactory {
        TJsonOutputProtocolFactory {}
    }
}

impl TOutputProtocolFactory for TJsonOutputProtocolFactory {
    fn create(
        &self,
        transport: Box<dyn TWriteTransport + Send>,
    ) -> Box<dyn TOutputProtocol + Send> {
        Box::new(TJsonOutputProtocol::new(transport))
    }
}

/// Read messages encoded in the Thrift JSON protocol.
///
/// The input is read one byte at a time, without any buffering beyond a
/// single byte of lookahead, so the transport is left right after the last
/// byte of the struct or message that was read. Whitespace between tokens is
/// not accepted, as in the other Thrift libraries.
///
/// A struct can be read on its own, without a message around it.
///
/// # Examples
///
/// ```
/// use thrift::protocol::{TInputProtocol, TJsonInputProtocol, TType};
///
/// let mut protocol = TJsonInputProtocol::new(&br#"{"1":{"i32":5}}"#[..]);
///
/// protocol.read_struct_begin().unwrap();
/// let field = protocol.read_field_begin().unwrap();
/// assert_eq!(field.field_type, TType::I32);
/// assert_eq!(field.id, Some(1));
/// assert_eq!(protocol.read_i32().unwrap(), 5);
/// protocol.read_field_end().unwrap();
/// assert_eq!(protocol.read_field_begin().unwrap().field_type, TType::Stop);
/// protocol.read_struct_end().unwrap();
/// ```
#[derive(Debug)]
pub struct TJsonInputProtocol<T>
where
    T: TReadTransport,
{
    transport: T,
    lookahead: Option<u8>,
    context: JsonContextStack,
    config: TConfiguration,
    recursion_depth: usize,
}

impl<T> TJsonInputProtocol<T>
where
    T: TReadTransport,
{
    /// Create a `TJsonInputProtocol` that reads bytes from `transport`.
    pub fn new(transport: T) -> Self {
        Self::with_config(transport, TConfiguration::default())
    }

    /// Create a `TJsonInputProtocol` with a custom configuration.
    pub fn with_config(transport: T, config: TConfiguration) -> Self {
        TJsonInputProtocol {
            transport,
            lookahead: None,
            context: JsonContextStack::new(),
            config,
            recursion_depth: 0,
        }
    }

    fn check_recursion_depth(&self) -> crate::Result<()> {
        if let Some(limit) = self.config.max_recursion_depth() {
            if self.recursion_depth >= limit {
                return Err(new_protocol_error(
                    ProtocolErrorKind::DepthLimit,
                    format!("Maximum recursion depth {} exceeded", limit),
                ));
            }
        }
        Ok(())
    }

    fn check_string_length(&self, length: usize) -> crate::Result<()> {
        if let Some(max_size) = self.config.max_string_size() {
            if length > max_size {
                return Err(new_protocol_error(
                    ProtocolErrorKind::SizeLimit,
                    format!(
                        "JSON value length exceeds maximum allowed size of {}",
                        max_size
                    ),
                ));
            }
        }
        Ok(())
    }

    fn read_transport_byte(&mut self) -> crate::Result<u8> {
        let mut buf = [0u8; 1];
        self.transport.read_exact(&mut buf)?;
        Ok(buf[0])
    }

    fn peek_byte(&mut self) -> crate::Result<u8> {
        match self.lookahead {
            Some(b) => Ok(b),
            None => {
                let b = self.read_transport_byte()?;
                self.lookahead = Some(b);
                Ok(b)
            }
        }
    }

    fn next_byte(&mut self) -> crate::Result<u8> {
        match self.lookahead.take() {
            Some(b) => Ok(b),
            None => self.read_transport_byte(),
        }
    }

    fn read_syntax_char(&mut self, expected: u8) -> crate::Result<()> {
        let b = self.next_byte()?;
        if b == expected {
            Ok(())
        } else {
            Err(invalid_data(format!(
                "Expected {:?}; got {:?}",
                char::from(expected),
                char::from(b)
            )))
        }
    }

    fn read_context(&mut self) -> crate::Result<()> {
        match self.context.current.next_separator() {
            Some(separator) => self.read_syntax_char(separator),
            None => Ok(()),
        }
    }

    fn read_escaped_code_unit(&mut self) -> crate::Result<u16> {
        let mut code_unit = 0u16;
        for _ in 0..4 {
            let b = self.next_byte()?;
            code_unit = (code_unit << 4) | hex_value(b)?;
        }
        Ok(code_unit)
    }

    fn read_json_string(&mut self, skip_context: bool) -> crate::Result<Vec<u8>> {
        if !skip_context {
            self.read_context()?;
        }
        self.read_syntax_char(b'"')?;
        let mut out = Vec::new();
        let mut high_surrogate: Option<u16> = None;
        loop {
            let mut b = self.next_byte()?;
            if b == b'"' {
                break;
            }
            if b == b'\\' {
                b = self.next_byte()?;
                if b == b'u' {
                    let code_unit = self.read_escaped_code_unit()?;
                    let code_point = if is_high_surrogate(code_unit) {
                        if high_surrogate.replace(code_unit).is_some() {
                            return Err(invalid_data("Expected low surrogate char"));
                        }
                        continue;
                    } else if is_low_surrogate(code_unit) {
                        let high = high_surrogate
                            .take()
                            .ok_or_else(|| invalid_data("Expected high surrogate char"))?;
                        0x10000
                            + ((u32::from(high) - 0xD800) << 10)
                            + (u32::from(code_unit) - 0xDC00)
                    } else if high_surrogate.is_some() {
                        return Err(invalid_data("Expected low surrogate char"));
                    } else {
                        u32::from(code_unit)
                    };
                    let c = char::from_u32(code_point)
                        .ok_or_else(|| invalid_data("Invalid unicode escape"))?;
                    let mut buf = [0u8; 4];
                    out.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
                    self.check_string_length(out.len())?;
                    continue;
                }
                b = match b {
                    b'"' | b'\\' | b'/' => b,
                    b'b' => 0x08,
                    b'f' => 0x0C,
                    b'n' => b'\n',
                    b'r' => b'\r',
                    b't' => b'\t',
                    other => {
                        return Err(invalid_data(format!(
                            "Expected control char, got {:?}",
                            char::from(other)
                        )));
                    }
                };
            }
            if high_surrogate.is_some() {
                return Err(invalid_data("Expected low surrogate char"));
            }
            out.push(b);
            self.check_string_length(out.len())?;
        }
        if high_surrogate.is_some() {
            return Err(invalid_data("Expected low surrogate char"));
        }
        Ok(out)
    }

    fn read_json_utf8_string(&mut self, skip_context: bool) -> crate::Result<String> {
        let bytes = self.read_json_string(skip_context)?;
        String::from_utf8(bytes).map_err(From::from)
    }

    fn read_json_numeric_chars(&mut self) -> crate::Result<String> {
        let mut number = String::new();
        loop {
            let b = match self.peek_byte() {
                Ok(b) => b,
                Err(crate::Error::Transport(e))
                    if e.kind == TransportErrorKind::EndOfFile && !number.is_empty() =>
                {
                    break;
                }
                Err(e) => return Err(e),
            };
            if !is_json_numeric(b) {
                break;
            }
            self.lookahead = None;
            number.push(char::from(b));
            self.check_string_length(number.len())?;
        }
        Ok(number)
    }

    fn read_json_integer<N: FromStr>(&mut self) -> crate::Result<N> {
        self.read_context()?;
        let quoted = self.context.current.escape_num();
        if quoted {
            self.read_syntax_char(b'"')?;
        }
        let number = self.read_json_numeric_chars()?;
        if quoted {
            self.read_syntax_char(b'"')?;
        }
        number
            .parse()
            .map_err(|_| invalid_data(format!("Expected numeric value; got {:?}", number)))
    }

    fn parse_double(number: &str) -> crate::Result<f64> {
        if !number.bytes().all(is_json_numeric) {
            return Err(invalid_data(format!(
                "Expected numeric value; got {:?}",
                number
            )));
        }
        number
            .parse()
            .map_err(|_| invalid_data(format!("Expected numeric value; got {:?}", number)))
    }

    fn read_json_double(&mut self) -> crate::Result<f64> {
        self.read_context()?;
        if self.peek_byte()? == b'"' {
            let number = self.read_json_utf8_string(true)?;
            match number.as_str() {
                "NaN" => Ok(f64::NAN),
                "Infinity" => Ok(f64::INFINITY),
                "-Infinity" => Ok(f64::NEG_INFINITY),
                _ if self.context.current.escape_num() => Self::parse_double(&number),
                _ => Err(invalid_data("Numeric data unexpectedly quoted")),
            }
        } else {
            if self.context.current.escape_num() {
                self.read_syntax_char(b'"')?;
            }
            let number = self.read_json_numeric_chars()?;
            Self::parse_double(&number)
        }
    }

    fn read_json_base64(&mut self) -> crate::Result<Vec<u8>> {
        let mut encoded = self.read_json_string(false)?;
        for _ in 0..2 {
            if encoded.last() == Some(&b'=') {
                encoded.pop();
            }
        }
        if encoded.len() % 4 == 1 {
            encoded.pop();
        }
        BASE64
            .decode(&encoded)
            .map_err(|e| invalid_data(format!("Invalid base64 data: {}", e)))
    }

    fn read_json_object_start(&mut self) -> crate::Result<()> {
        self.read_context()?;
        self.read_syntax_char(b'{')?;
        self.context.push(JsonContext::pair());
        Ok(())
    }

    fn read_json_object_end(&mut self) -> crate::Result<()> {
        self.read_syntax_char(b'}')?;
        self.context.pop();
        Ok(())
    }

    fn read_json_array_start(&mut self) -> crate::Result<()> {
        self.read_context()?;
        self.read_syntax_char(b'[')?;
        self.context.push(JsonContext::list());
        Ok(())
    }

    fn read_json_array_end(&mut self) -> crate::Result<()> {
        self.read_syntax_char(b']')?;
        self.context.pop();
        Ok(())
    }

    fn read_type_tag(&mut self) -> crate::Result<TType> {
        let tag = self.read_json_string(false)?;
        tag_to_type(&tag)
    }

    fn read_container_size(&mut self) -> crate::Result<i32> {
        let size: i64 = self.read_json_integer()?;
        i32::try_from(size).map_err(|_| {
            let kind = if size < 0 {
                ProtocolErrorKind::NegativeSize
            } else {
                ProtocolErrorKind::SizeLimit
            };
            new_protocol_error(kind, format!("Invalid container size: {}", size))
        })
    }

    fn read_list_set_begin(&mut self) -> crate::Result<(TType, i32)> {
        self.read_json_array_start()?;
        let element_type = self.read_type_tag()?;
        let size = self.read_container_size()?;
        let min_element_size = self.min_serialized_size(element_type);
        super::check_container_size(&self.config, size, min_element_size)?;
        Ok((element_type, size))
    }
}

impl<T> TInputProtocol for TJsonInputProtocol<T>
where
    T: TReadTransport,
{
    fn read_message_begin(&mut self) -> crate::Result<TMessageIdentifier> {
        self.context.reset();
        self.read_json_array_start()?;
        let version: i64 = self.read_json_integer()?;
        if version != THRIFT_JSON_VERSION {
            return Err(new_protocol_error(
                ProtocolErrorKind::BadVersion,
                "Message contained bad version.",
            ));
        }
        let name = self.read_json_utf8_string(false)?;
        let message_type: u8 = self.read_json_integer()?;
        let message_type = TMessageType::try_from(message_type)?;
        let sequence_number: i32 = self.read_json_integer()?;
        Ok(TMessageIdentifier::new(name, message_type, sequence_number))
    }

    fn read_message_end(&mut self) -> crate::Result<()> {
        self.read_json_array_end()
    }

    fn read_struct_begin(&mut self) -> crate::Result<Option<TStructIdentifier>> {
        self.check_recursion_depth()?;
        self.read_json_object_start()?;
        self.recursion_depth += 1;
        Ok(None)
    }

    fn read_struct_end(&mut self) -> crate::Result<()> {
        self.read_json_object_end()?;
        self.recursion_depth = self.recursion_depth.saturating_sub(1);
        Ok(())
    }

    fn read_field_begin(&mut self) -> crate::Result<TFieldIdentifier> {
        if self.peek_byte()? == b'}' {
            return Ok(
                TFieldIdentifier::new::<Option<String>, String, Option<i16>>(
                    None,
                    TType::Stop,
                    None,
                ),
            );
        }
        let id: i16 = self.read_json_integer()?;
        self.read_json_object_start()?;
        let field_type = self.read_type_tag()?;
        Ok(TFieldIdentifier::new::<Option<String>, String, i16>(
            None, field_type, id,
        ))
    }

    fn read_field_end(&mut self) -> crate::Result<()> {
        self.read_json_object_end()
    }

    fn read_bool(&mut self) -> crate::Result<bool> {
        let value: i64 = self.read_json_integer()?;
        match value {
            0 => Ok(false),
            1 => Ok(true),
            other => Err(invalid_data(format!("cannot convert {} into bool", other))),
        }
    }

    fn read_bytes(&mut self) -> crate::Result<Vec<u8>> {
        self.read_json_base64()
    }

    fn skip_binary(&mut self) -> crate::Result<()> {
        self.read_json_string(false).map(|_| ())
    }

    fn read_i8(&mut self) -> crate::Result<i8> {
        self.read_json_integer()
    }

    fn read_i16(&mut self) -> crate::Result<i16> {
        self.read_json_integer()
    }

    fn read_i32(&mut self) -> crate::Result<i32> {
        self.read_json_integer()
    }

    fn read_i64(&mut self) -> crate::Result<i64> {
        self.read_json_integer()
    }

    fn read_double(&mut self) -> crate::Result<f64> {
        self.read_json_double()
    }

    fn read_uuid(&mut self) -> crate::Result<uuid::Uuid> {
        let s = self.read_json_utf8_string(false)?;
        uuid::Uuid::parse_str(&s).map_err(From::from)
    }

    fn read_string(&mut self) -> crate::Result<String> {
        self.read_json_utf8_string(false)
    }

    fn read_list_begin(&mut self) -> crate::Result<TListIdentifier> {
        let (element_type, size) = self.read_list_set_begin()?;
        Ok(TListIdentifier::new(element_type, size))
    }

    fn read_list_end(&mut self) -> crate::Result<()> {
        self.read_json_array_end()
    }

    fn read_set_begin(&mut self) -> crate::Result<TSetIdentifier> {
        let (element_type, size) = self.read_list_set_begin()?;
        Ok(TSetIdentifier::new(element_type, size))
    }

    fn read_set_end(&mut self) -> crate::Result<()> {
        self.read_json_array_end()
    }

    fn read_map_begin(&mut self) -> crate::Result<TMapIdentifier> {
        self.read_json_array_start()?;
        let key_type = self.read_type_tag()?;
        let value_type = self.read_type_tag()?;
        let size = self.read_container_size()?;
        self.read_json_object_start()?;
        let element_size =
            self.min_serialized_size(key_type) + self.min_serialized_size(value_type);
        super::check_container_size(&self.config, size, element_size)?;
        Ok(TMapIdentifier::new(key_type, value_type, size))
    }

    fn read_map_end(&mut self) -> crate::Result<()> {
        self.read_json_object_end()?;
        self.read_json_array_end()
    }

    fn read_byte(&mut self) -> crate::Result<u8> {
        self.read_json_integer()
    }

    fn min_serialized_size(&self, field_type: TType) -> usize {
        match field_type {
            TType::String | TType::Struct | TType::Map | TType::Set | TType::List => 2,
            TType::Uuid => 38,
            _ => 1,
        }
    }
}

/// Factory for creating instances of `TJsonInputProtocol`.
#[derive(Default)]
pub struct TJsonInputProtocolFactory;

impl TJsonInputProtocolFactory {
    /// Create a `TJsonInputProtocolFactory`.
    pub fn new() -> TJsonInputProtocolFactory {
        TJsonInputProtocolFactory {}
    }
}

impl TInputProtocolFactory for TJsonInputProtocolFactory {
    fn create(&self, transport: Box<dyn TReadTransport + Send>) -> Box<dyn TInputProtocol + Send> {
        Box::new(TJsonInputProtocol::new(transport))
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::protocol::{TMessageType, TMultiplexedOutputProtocol};
    use crate::transport::{ReadHalf, TBufferChannel, TIoChannel, WriteHalf};

    fn output() -> TJsonOutputProtocol<WriteHalf<TBufferChannel>> {
        output_with_config(TConfiguration::default())
    }

    fn output_with_config(
        config: TConfiguration,
    ) -> TJsonOutputProtocol<WriteHalf<TBufferChannel>> {
        let (_, w_mem) = TBufferChannel::with_capacity(0, 4096).split().unwrap();
        TJsonOutputProtocol::with_config(w_mem, config)
    }

    fn written(o_prot: &TJsonOutputProtocol<WriteHalf<TBufferChannel>>) -> String {
        String::from_utf8(o_prot.transport.write_bytes()).unwrap()
    }

    fn assert_written<F>(expected: &str, write_fn: F)
    where
        F: FnOnce(&mut TJsonOutputProtocol<WriteHalf<TBufferChannel>>) -> crate::Result<()>,
    {
        let mut o_prot = output();
        assert_success!(write_fn(&mut o_prot));
        assert_eq!(written(&o_prot), expected);
    }

    fn write_field<F>(
        o_prot: &mut TJsonOutputProtocol<WriteHalf<TBufferChannel>>,
        field_type: TType,
        id: i16,
        write_value: F,
    ) -> crate::Result<()>
    where
        F: FnOnce(&mut TJsonOutputProtocol<WriteHalf<TBufferChannel>>) -> crate::Result<()>,
    {
        o_prot.write_field_begin(&TFieldIdentifier::new("f", field_type, id))?;
        write_value(o_prot)?;
        o_prot.write_field_end()
    }

    #[test]
    fn must_write_integers() {
        assert_written("1", |o| o.write_bool(true));
        assert_written("0", |o| o.write_bool(false));
        assert_written("-128", |o| o.write_i8(i8::MIN));
        assert_written("32767", |o| o.write_i16(i16::MAX));
        assert_written("-2147483648", |o| o.write_i32(i32::MIN));
        assert_written("-9223372036854775808", |o| o.write_i64(i64::MIN));
        assert_written("255", |o| o.write_byte(0xFF));
    }

    #[test]
    fn must_write_doubles() {
        assert_written("\"NaN\"", |o| o.write_double(f64::NAN));
        assert_written("\"Infinity\"", |o| o.write_double(f64::INFINITY));
        assert_written("\"-Infinity\"", |o| o.write_double(f64::NEG_INFINITY));
        assert_written("0", |o| o.write_double(0.0));
        assert_written("-0", |o| o.write_double(-0.0));
        assert_written("0.1", |o| o.write_double(0.1));
        assert_written("-2.5", |o| o.write_double(-2.5));
        assert_written("3.141592653589793", |o| {
            o.write_double(std::f64::consts::PI)
        });
        assert_written("1e305", |o| o.write_double(1e305));
        assert_written("1e-305", |o| o.write_double(1e-305));
        assert_written("1e16", |o| o.write_double(1e16));
        assert_written("1000000000000000", |o| o.write_double(1e15));
        assert_written("0.00001", |o| o.write_double(1e-5));
        assert_written("1.7976931348623157e308", |o| o.write_double(f64::MAX));
    }

    #[test]
    fn must_write_strings_with_the_same_escapes_as_other_languages() {
        assert_written(r#""""#, |o| o.write_string(""));
        assert_written(r#""JSON THIS! \"\u0001""#, |o| {
            o.write_string("JSON THIS! \"\u{1}")
        });
        assert_written("\"\u{d7}\\n\\u0007\\t\"", |o| {
            o.write_string("\u{d7}\n\u{7}\t")
        });
        assert_written(r#""\b\f\r\\/""#, |o| o.write_string("\u{8}\u{c}\r\\/"));
        assert_written("\"\\u0000\\u000b\\u001f \u{7f}\"", |o| {
            o.write_string("\u{0}\u{b}\u{1f} \u{7f}")
        });
        assert_written("\"\u{e01} \u{1d53e}\"", |o| {
            o.write_string("\u{e01} \u{1d53e}")
        });
    }

    #[test]
    fn must_write_binary_as_unpadded_base64() {
        assert_written(r#""""#, |o| o.write_bytes(&[]));
        assert_written(r#""AA""#, |o| o.write_bytes(&[0]));
        assert_written(r#""AAE""#, |o| o.write_bytes(&[0, 1]));
        assert_written(r#""AAEC""#, |o| o.write_bytes(&[0, 1, 2]));
        assert_written(r#""AAECAw""#, |o| o.write_bytes(&[0, 1, 2, 3]));
        assert_written(r#""AAECAwQ""#, |o| o.write_bytes(&[0, 1, 2, 3, 4]));
        assert_written(r#""AAECAwQF""#, |o| o.write_bytes(&[0, 1, 2, 3, 4, 5]));
        assert_written(r#""AQIDrQ""#, |o| o.write_bytes(&[1, 2, 3, 0xAD]));
        assert_written(r#""AP+AAQ""#, |o| o.write_bytes(&[0, 255, 128, 1]));
    }

    #[test]
    fn must_write_uuid_as_lowercase_string() {
        let uuid = uuid::Uuid::parse_str("00112233-4455-6677-8899-AABBCCDDEEFF").unwrap();
        assert_written(r#""00112233-4455-6677-8899-aabbccddeeff""#, |o| {
            o.write_uuid(&uuid)
        });
    }

    #[test]
    fn must_write_top_level_struct_without_message() {
        assert_written(
            r#"{"1":{"i32":9},"2":{"str":"id-42"},"12":{"rec":{}}}"#,
            |o| {
                o.write_struct_begin(&TStructIdentifier::new("Outer"))?;
                write_field(o, TType::I32, 1, |o| o.write_i32(9))?;
                write_field(o, TType::String, 2, |o| o.write_string("id-42"))?;
                write_field(o, TType::Struct, 12, |o| {
                    o.write_struct_begin(&TStructIdentifier::new("Empty"))?;
                    o.write_field_stop()?;
                    o.write_struct_end()
                })?;
                o.write_field_stop()?;
                o.write_struct_end()
            },
        );
    }

    #[test]
    fn must_write_nested_structs() {
        assert_written(r#"{"1":{"i32":0},"3":{"rec":{"1":{"str":""}}}}"#, |o| {
            o.write_struct_begin(&TStructIdentifier::new("Outer"))?;
            write_field(o, TType::I32, 1, |o| o.write_i32(0))?;
            write_field(o, TType::Struct, 3, |o| {
                o.write_struct_begin(&TStructIdentifier::new("Inner"))?;
                write_field(o, TType::String, 1, |o| o.write_string(""))?;
                o.write_field_stop()?;
                o.write_struct_end()
            })?;
            o.write_field_stop()?;
            o.write_struct_end()
        });
    }

    #[test]
    fn must_write_every_field_type_tag() {
        assert_written(
            r#"{"1":{"tf":1},"2":{"i8":-1},"3":{"i16":2},"4":{"i32":3},"5":{"i64":4},"6":{"dbl":0.5},"7":{"str":"s"},"8":{"uid":"00000000-0000-0000-0000-000000000000"}}"#,
            |o| {
                o.write_struct_begin(&TStructIdentifier::new("S"))?;
                write_field(o, TType::Bool, 1, |o| o.write_bool(true))?;
                write_field(o, TType::I08, 2, |o| o.write_i8(-1))?;
                write_field(o, TType::I16, 3, |o| o.write_i16(2))?;
                write_field(o, TType::I32, 4, |o| o.write_i32(3))?;
                write_field(o, TType::I64, 5, |o| o.write_i64(4))?;
                write_field(o, TType::Double, 6, |o| o.write_double(0.5))?;
                write_field(o, TType::String, 7, |o| o.write_string("s"))?;
                write_field(o, TType::Uuid, 8, |o| o.write_uuid(&uuid::Uuid::nil()))?;
                o.write_field_stop()?;
                o.write_struct_end()
            },
        );
    }

    #[test]
    fn must_write_lists_and_sets() {
        assert_written(r#"["rec",0]"#, |o| {
            o.write_list_begin(&TListIdentifier::new(TType::Struct, 0))?;
            o.write_list_end()
        });
        assert_written(r#"["i8",3,1,2,3]"#, |o| {
            o.write_list_begin(&TListIdentifier::new(TType::I08, 3))?;
            o.write_i8(1)?;
            o.write_i8(2)?;
            o.write_i8(3)?;
            o.write_list_end()
        });
        assert_written(r#"["str",2,"a","b"]"#, |o| {
            o.write_set_begin(&TSetIdentifier::new(TType::String, 2))?;
            o.write_string("a")?;
            o.write_string("b")?;
            o.write_set_end()
        });
        assert_written(r#"["lst",2,["i32",1,7],["i32",0]]"#, |o| {
            o.write_list_begin(&TListIdentifier::new(TType::List, 2))?;
            o.write_list_begin(&TListIdentifier::new(TType::I32, 1))?;
            o.write_i32(7)?;
            o.write_list_end()?;
            o.write_list_begin(&TListIdentifier::new(TType::I32, 0))?;
            o.write_list_end()?;
            o.write_list_end()
        });
        assert_written(
            r#"["rec",2,{"1":{"str":"app1"},"3":{"i64":100}},{}]"#,
            |o| {
                o.write_list_begin(&TListIdentifier::new(TType::Struct, 2))?;
                o.write_struct_begin(&TStructIdentifier::new("Window"))?;
                write_field(o, TType::String, 1, |o| o.write_string("app1"))?;
                write_field(o, TType::I64, 3, |o| o.write_i64(100))?;
                o.write_field_stop()?;
                o.write_struct_end()?;
                o.write_struct_begin(&TStructIdentifier::new("Window"))?;
                o.write_field_stop()?;
                o.write_struct_end()?;
                o.write_list_end()
            },
        );
    }

    #[test]
    fn must_write_maps_with_quoted_numeric_keys() {
        assert_written(
            r#"["str","str",2,{"PST":"Pacific","EST":"Eastern"}]"#,
            |o| {
                o.write_map_begin(&TMapIdentifier::new(TType::String, TType::String, 2))?;
                o.write_string("PST")?;
                o.write_string("Pacific")?;
                o.write_string("EST")?;
                o.write_string("Eastern")?;
                o.write_map_end()
            },
        );
        assert_written(r#"["i32","i64",2,{"1":-1,"2":-2}]"#, |o| {
            o.write_map_begin(&TMapIdentifier::new(TType::I32, TType::I64, 2))?;
            o.write_i32(1)?;
            o.write_i64(-1)?;
            o.write_i32(2)?;
            o.write_i64(-2)?;
            o.write_map_end()
        });
        assert_written(r#"["tf","dbl",2,{"1":"NaN","0":0.5}]"#, |o| {
            o.write_map_begin(&TMapIdentifier::new(TType::Bool, TType::Double, 2))?;
            o.write_bool(true)?;
            o.write_double(f64::NAN)?;
            o.write_bool(false)?;
            o.write_double(0.5)?;
            o.write_map_end()
        });
        assert_written(r#"["dbl","map",1,{"1.5":["i8","i8",0,{}]}]"#, |o| {
            o.write_map_begin(&TMapIdentifier::new(TType::Double, TType::Map, 1))?;
            o.write_double(1.5)?;
            o.write_map_begin(&TMapIdentifier::new(TType::I08, TType::I08, 0))?;
            o.write_map_end()?;
            o.write_map_end()
        });
    }

    #[test]
    fn must_write_message() {
        assert_written(r#"[1,"test",1,1,{"0":{"str":"x"}}]"#, |o| {
            o.write_message_begin(&TMessageIdentifier::new("test", TMessageType::Call, 1))?;
            o.write_struct_begin(&TStructIdentifier::new("args"))?;
            write_field(o, TType::String, 0, |o| o.write_string("x"))?;
            o.write_field_stop()?;
            o.write_struct_end()?;
            o.write_message_end()
        });
        assert_written(r#"[1,"o",4,-1,{}]"#, |o| {
            o.write_message_begin(&TMessageIdentifier::new("o", TMessageType::OneWay, -1))?;
            o.write_struct_begin(&TStructIdentifier::new("args"))?;
            o.write_field_stop()?;
            o.write_struct_end()?;
            o.write_message_end()
        });
    }

    #[test]
    fn must_write_messages_back_to_back() {
        let mut o_prot = output();
        for seq in 1..=2 {
            let ident = TMessageIdentifier::new("m", TMessageType::Reply, seq);
            assert_success!(o_prot.write_message_begin(&ident));
            assert_success!(o_prot.write_struct_begin(&TStructIdentifier::new("r")));
            assert_success!(o_prot.write_field_stop());
            assert_success!(o_prot.write_struct_end());
            assert_success!(o_prot.write_message_end());
        }
        assert_eq!(written(&o_prot), r#"[1,"m",2,1,{}][1,"m",2,2,{}]"#);
    }

    #[test]
    fn must_prefix_multiplexed_message_names() {
        let (r_mem, w_mem) = TBufferChannel::with_capacity(0, 4096).split().unwrap();
        let mut o_prot = TMultiplexedOutputProtocol::new("svc", TJsonOutputProtocol::new(w_mem));
        let ident = TMessageIdentifier::new("ping", TMessageType::Call, 3);
        assert_success!(o_prot.write_message_begin(&ident));
        assert_success!(o_prot.write_struct_begin(&TStructIdentifier::new("args")));
        assert_success!(o_prot.write_field_stop());
        assert_success!(o_prot.write_struct_end());
        assert_success!(o_prot.write_message_end());
        assert_eq!(r_mem.write_bytes(), br#"[1,"svc:ping",1,3,{}]"#);
    }

    #[test]
    fn must_reject_untagged_types() {
        let mut o_prot = output();
        assert_success!(o_prot.write_struct_begin(&TStructIdentifier::new("S")));
        match o_prot.write_field_begin(&TFieldIdentifier::new("v", TType::Void, 1)) {
            Err(crate::Error::Protocol(e)) => assert_eq!(e.kind, ProtocolErrorKind::NotImplemented),
            other => panic!("expected NotImplemented, got {:?}", other),
        }
        match o_prot.write_list_begin(&TListIdentifier::new(TType::Stop, 0)) {
            Err(crate::Error::Protocol(e)) => assert_eq!(e.kind, ProtocolErrorKind::NotImplemented),
            other => panic!("expected NotImplemented, got {:?}", other),
        }
    }

    #[test]
    fn must_reject_field_without_id() {
        let mut o_prot = output();
        let ident = TFieldIdentifier::new::<_, &str, Option<i16>>("f", TType::I32, None);
        assert!(o_prot.write_field_begin(&ident).is_err());
    }

    #[test]
    fn must_reject_map_without_types() {
        let mut o_prot = output();
        let ident = TMapIdentifier::new(None, None, 0);
        assert!(o_prot.write_map_begin(&ident).is_err());
    }

    #[test]
    fn must_enforce_recursion_depth_limit() {
        let config = TConfiguration::builder()
            .max_recursion_depth(Some(2))
            .build()
            .unwrap();
        let mut o_prot = output_with_config(config);
        let ident = TStructIdentifier::new("S");

        assert_success!(o_prot.write_struct_begin(&ident));
        assert_success!(o_prot.write_struct_begin(&ident));
        match o_prot.write_struct_begin(&ident) {
            Err(crate::Error::Protocol(e)) => assert_eq!(e.kind, ProtocolErrorKind::DepthLimit),
            other => panic!("expected DepthLimit, got {:?}", other),
        }
        assert_success!(o_prot.write_struct_end());
        assert_success!(o_prot.write_struct_end());
        assert_eq!(o_prot.recursion_depth, 0);
        assert_success!(o_prot.write_struct_begin(&ident));
    }

    #[test]
    fn must_create_protocol_from_factory() {
        let factory = TJsonOutputProtocolFactory::new();
        let mut o_prot = factory.create(Box::new(Vec::new()));
        assert_success!(o_prot.write_i32(1));
        assert_success!(o_prot.flush());
    }

    fn input(s: &str) -> TJsonInputProtocol<&[u8]> {
        TJsonInputProtocol::new(s.as_bytes())
    }

    fn input_with_config(s: &str, config: TConfiguration) -> TJsonInputProtocol<&[u8]> {
        TJsonInputProtocol::with_config(s.as_bytes(), config)
    }

    fn assert_fully_read(i_prot: &TJsonInputProtocol<&[u8]>) {
        assert_eq!(i_prot.lookahead, None);
        assert!(i_prot.transport.is_empty(), "unread input left");
    }

    fn assert_error_kind<V: std::fmt::Debug>(result: crate::Result<V>, kind: ProtocolErrorKind) {
        match result {
            Err(crate::Error::Protocol(e)) => assert_eq!(e.kind, kind, "{}", e.message),
            other => panic!("expected protocol error {:?}, got {:?}", kind, other),
        }
    }

    fn assert_invalid<V: std::fmt::Debug>(result: crate::Result<V>) {
        assert_error_kind(result, ProtocolErrorKind::InvalidData);
    }

    fn round_trip_channel() -> (
        TJsonInputProtocol<ReadHalf<TBufferChannel>>,
        TJsonOutputProtocol<WriteHalf<TBufferChannel>>,
    ) {
        let (r_mem, w_mem) = TBufferChannel::with_capacity(4096, 4096).split().unwrap();
        (
            TJsonInputProtocol::new(r_mem),
            TJsonOutputProtocol::new(w_mem),
        )
    }

    #[test]
    fn must_read_integers() {
        assert!(input("1").read_bool().unwrap());
        assert!(!input("0").read_bool().unwrap());
        assert_eq!(input("-128").read_i8().unwrap(), i8::MIN);
        assert_eq!(input("32767").read_i16().unwrap(), i16::MAX);
        assert_eq!(input("-2147483648").read_i32().unwrap(), i32::MIN);
        assert_eq!(input("+7").read_i32().unwrap(), 7);
        assert_eq!(input("-9223372036854775808").read_i64().unwrap(), i64::MIN);
        assert_eq!(input("255").read_byte().unwrap(), 0xFF);
    }

    #[test]
    fn must_reject_invalid_integers() {
        assert_invalid(input("2").read_bool());
        assert_invalid(input("849").read_i8());
        assert_invalid(input("32768").read_i16());
        assert_invalid(input("16.77216").read_i32());
        assert_invalid(input("9223372036854775808").read_i64());
        assert_invalid(input("1e3").read_i64());
        assert_invalid(input("]").read_i32());
        assert_invalid(input("\"1\"").read_i32());
        assert_invalid(input("true").read_bool());
        assert!(input("").read_i32().is_err());
    }

    #[test]
    fn must_read_doubles() {
        assert!(input("\"NaN\"").read_double().unwrap().is_nan());
        assert_eq!(input("\"Infinity\"").read_double().unwrap(), f64::INFINITY);
        assert_eq!(
            input("\"-Infinity\"").read_double().unwrap(),
            f64::NEG_INFINITY
        );
        assert_eq!(input("0.5").read_double().unwrap(), 0.5);
        assert_eq!(input("1e305").read_double().unwrap(), 1e305);
        assert_eq!(
            input("9.9999999999999994e+304").read_double().unwrap(),
            1e305
        );
        assert_eq!(input("1.0E305").read_double().unwrap(), 1e305);
        assert_eq!(input("3").read_double().unwrap(), 3.0);
        assert!(input("-0").read_double().unwrap().is_sign_negative());
    }

    #[test]
    fn must_reject_invalid_doubles() {
        assert_invalid(input("\"1.5\"").read_double());
        assert_invalid(input("\"nan\"").read_double());
        assert_invalid(input("1e").read_double());
        assert_invalid(input("1.2.3").read_double());
        assert_invalid(input("NaN").read_double());
    }

    #[test]
    fn must_round_trip_doubles() {
        let values = [
            0.0,
            -0.0,
            0.1,
            1.0 / 3.0,
            -2.5,
            1e-5,
            9.999e-6,
            1e15,
            1e16,
            1e305,
            1e-305,
            f64::MAX,
            f64::MIN_POSITIVE,
            5e-324,
            f64::INFINITY,
            f64::NEG_INFINITY,
        ];
        for &value in &values {
            let mut buf = Vec::new();
            TJsonOutputProtocol::new(&mut buf)
                .write_double(value)
                .unwrap();
            let read = TJsonInputProtocol::new(&buf[..]).read_double().unwrap();
            assert_eq!(read.to_bits(), value.to_bits(), "{}", value);
        }
    }

    #[test]
    fn must_read_strings() {
        assert_eq!(input(r#""""#).read_string().unwrap(), "");
        assert_eq!(
            input(r#""\"\\\/\b\f\n\r\t""#).read_string().unwrap(),
            "\"\\/\u{8}\u{c}\n\r\t"
        );
        assert_eq!(
            input(r#""JSON THIS! \"\u0001""#).read_string().unwrap(),
            "JSON THIS! \"\u{1}"
        );
        assert_eq!(
            input(r#""\u0041\u00e9\u00E9\u0000""#)
                .read_string()
                .unwrap(),
            "Aéé\u{0}"
        );
        assert_eq!(
            input("\"caf\u{e9} \u{1f680}\"").read_string().unwrap(),
            "café 🚀"
        );
        assert_eq!(
            input(r#""\u0e01 \ud835\udd3e""#)
                .read_string()
                .unwrap()
                .as_bytes(),
            [0xE0, 0xB8, 0x81, 0x20, 0xF0, 0x9D, 0x94, 0xBE]
        );
    }

    #[test]
    fn must_round_trip_strings() {
        let values = [
            "",
            "plain",
            "JSON THIS! \"\u{1}",
            "\u{d7}\n\u{7}\t",
            "back\\slash /slash",
            "\u{0}\u{1f}\u{7f}",
            "\u{e01} \u{1d53e} 🎉",
        ];
        for value in values {
            let (mut i_prot, mut o_prot) = round_trip_channel();
            o_prot.write_string(value).unwrap();
            o_prot.transport.copy_write_buffer_to_read_buffer();
            assert_eq!(i_prot.read_string().unwrap(), value);
        }
    }

    #[test]
    fn must_reject_invalid_strings() {
        assert_invalid(input(r#""\ud834""#).read_string());
        assert_invalid(input(r#""\ud834a""#).read_string());
        assert_invalid(input(r#""\ud834\u0041\udd1e""#).read_string());
        assert_invalid(input(r#""\ud834\ud834\udd1e""#).read_string());
        assert_invalid(input(r#""\udd1e""#).read_string());
        assert_invalid(input(r#""\x""#).read_string());
        assert_invalid(input(r#""\u12g4""#).read_string());
        assert_invalid(input("'single'").read_string());
        assert_invalid(TJsonInputProtocol::new(&b"\"\xff\""[..]).read_string());
        assert!(input("\"unterminated").read_string().is_err());
    }

    #[test]
    fn must_read_binary_with_or_without_padding() {
        assert_eq!(input(r#""""#).read_bytes().unwrap(), Vec::<u8>::new());
        assert_eq!(input(r#""AQIDrQ""#).read_bytes().unwrap(), [1, 2, 3, 0xAD]);
        assert_eq!(
            input(r#""AQIDrQ==""#).read_bytes().unwrap(),
            [1, 2, 3, 0xAD]
        );
        assert_eq!(
            input(r#""AP+AAQ==""#).read_bytes().unwrap(),
            [0, 255, 128, 1]
        );
        assert_eq!(
            input(r#""AAECAwQ=""#).read_bytes().unwrap(),
            [0, 1, 2, 3, 4]
        );
        assert_eq!(
            input(r#""AAECAwQF""#).read_bytes().unwrap(),
            [0, 1, 2, 3, 4, 5]
        );
        for padded in ["QWE=", "QQ==", "=", "==", "===", "A", "AAAAA"] {
            assert_success!(input(&format!("\"{}\"", padded)).read_bytes());
        }
        assert_invalid(input(r#""====""#).read_bytes());
        assert_invalid(input(r#""!!!!""#).read_bytes());
    }

    #[test]
    fn must_round_trip_binary() {
        for len in 0..10u8 {
            let value: Vec<u8> = (0..len).map(|b| b.wrapping_mul(97)).collect();
            let (mut i_prot, mut o_prot) = round_trip_channel();
            o_prot.write_bytes(&value).unwrap();
            o_prot.transport.copy_write_buffer_to_read_buffer();
            assert_eq!(i_prot.read_bytes().unwrap(), value);
        }
    }

    #[test]
    fn must_read_uuids() {
        let expected = uuid::Uuid::parse_str("00112233-4455-6677-8899-aabbccddeeff").unwrap();
        assert_eq!(
            input(r#""00112233-4455-6677-8899-aabbccddeeff""#)
                .read_uuid()
                .unwrap(),
            expected
        );
        assert_eq!(
            input(r#""00112233-4455-6677-8899-AABBCCDDEEFF""#)
                .read_uuid()
                .unwrap(),
            expected
        );
        assert_invalid(input(r#""not-a-uuid""#).read_uuid());
    }

    #[test]
    fn must_enforce_string_size_limit() {
        let config = TConfiguration::builder()
            .max_string_size(Some(4))
            .build()
            .unwrap();
        assert_eq!(
            input_with_config(r#""abcd""#, config.clone())
                .read_string()
                .unwrap(),
            "abcd"
        );
        assert_error_kind(
            input_with_config(r#""abcde""#, config.clone()).read_string(),
            ProtocolErrorKind::SizeLimit,
        );
        assert_error_kind(
            input_with_config(r#""\u00e9\u00e9\u00e9""#, config.clone()).read_string(),
            ProtocolErrorKind::SizeLimit,
        );
        assert_error_kind(
            input_with_config(r#""AAAAAAAA""#, config.clone()).read_bytes(),
            ProtocolErrorKind::SizeLimit,
        );
        assert_error_kind(
            input_with_config(r#""AAAAAAAA""#, config.clone()).skip(TType::String),
            ProtocolErrorKind::SizeLimit,
        );
        assert_error_kind(
            input_with_config("12345", config).read_i64(),
            ProtocolErrorKind::SizeLimit,
        );
    }

    #[test]
    fn must_round_trip_message_with_every_type() {
        let (mut i_prot, mut o_prot) = round_trip_channel();
        let uuid = uuid::Uuid::parse_str("00112233-4455-6677-8899-aabbccddeeff").unwrap();

        let ident = TMessageIdentifier::new("everything", TMessageType::Reply, 42);
        o_prot.write_message_begin(&ident).unwrap();
        o_prot
            .write_struct_begin(&TStructIdentifier::new("S"))
            .unwrap();
        write_field(&mut o_prot, TType::Bool, 1, |o| o.write_bool(true)).unwrap();
        write_field(&mut o_prot, TType::I08, 2, |o| o.write_i8(-8)).unwrap();
        write_field(&mut o_prot, TType::I16, 3, |o| o.write_i16(-16)).unwrap();
        write_field(&mut o_prot, TType::I32, 4, |o| o.write_i32(-32)).unwrap();
        write_field(&mut o_prot, TType::I64, -5, |o| o.write_i64(i64::MAX)).unwrap();
        write_field(&mut o_prot, TType::Double, 6, |o| o.write_double(-0.25)).unwrap();
        write_field(&mut o_prot, TType::String, 7, |o| o.write_string("str\n")).unwrap();
        write_field(&mut o_prot, TType::String, 8, |o| {
            o.write_bytes(&[0, 1, 2, 3])
        })
        .unwrap();
        write_field(&mut o_prot, TType::Uuid, 9, |o| o.write_uuid(&uuid)).unwrap();
        write_field(&mut o_prot, TType::List, 10, |o| {
            o.write_list_begin(&TListIdentifier::new(TType::Struct, 2))?;
            for i in 0..2 {
                o.write_struct_begin(&TStructIdentifier::new("Inner"))?;
                write_field(o, TType::I32, 1, |o| o.write_i32(i))?;
                o.write_field_stop()?;
                o.write_struct_end()?;
            }
            o.write_list_end()
        })
        .unwrap();
        write_field(&mut o_prot, TType::Set, 11, |o| {
            o.write_set_begin(&TSetIdentifier::new(TType::Bool, 2))?;
            o.write_bool(false)?;
            o.write_bool(true)?;
            o.write_set_end()
        })
        .unwrap();
        write_field(&mut o_prot, TType::Map, 12, |o| {
            o.write_map_begin(&TMapIdentifier::new(TType::I16, TType::Map, 1))?;
            o.write_i16(-1)?;
            o.write_map_begin(&TMapIdentifier::new(TType::Double, TType::List, 1))?;
            o.write_double(f64::NEG_INFINITY)?;
            o.write_list_begin(&TListIdentifier::new(TType::String, 1))?;
            o.write_string("x")?;
            o.write_list_end()?;
            o.write_map_end()?;
            o.write_map_end()
        })
        .unwrap();
        o_prot.write_field_stop().unwrap();
        o_prot.write_struct_end().unwrap();
        o_prot.write_message_end().unwrap();
        o_prot.transport.copy_write_buffer_to_read_buffer();

        fn field(i_prot: &mut TJsonInputProtocol<ReadHalf<TBufferChannel>>, t: TType, id: i16) {
            let ident = i_prot.read_field_begin().unwrap();
            assert_eq!(ident.field_type, t);
            if t != TType::Stop {
                assert_eq!(ident.id, Some(id));
            }
        }

        assert_eq!(i_prot.read_message_begin().unwrap(), ident);
        assert_eq!(i_prot.read_struct_begin().unwrap(), None);
        field(&mut i_prot, TType::Bool, 1);
        assert!(i_prot.read_bool().unwrap());
        i_prot.read_field_end().unwrap();
        field(&mut i_prot, TType::I08, 2);
        assert_eq!(i_prot.read_i8().unwrap(), -8);
        i_prot.read_field_end().unwrap();
        field(&mut i_prot, TType::I16, 3);
        assert_eq!(i_prot.read_i16().unwrap(), -16);
        i_prot.read_field_end().unwrap();
        field(&mut i_prot, TType::I32, 4);
        assert_eq!(i_prot.read_i32().unwrap(), -32);
        i_prot.read_field_end().unwrap();
        field(&mut i_prot, TType::I64, -5);
        assert_eq!(i_prot.read_i64().unwrap(), i64::MAX);
        i_prot.read_field_end().unwrap();
        field(&mut i_prot, TType::Double, 6);
        assert_eq!(i_prot.read_double().unwrap(), -0.25);
        i_prot.read_field_end().unwrap();
        field(&mut i_prot, TType::String, 7);
        assert_eq!(i_prot.read_string().unwrap(), "str\n");
        i_prot.read_field_end().unwrap();
        field(&mut i_prot, TType::String, 8);
        assert_eq!(i_prot.read_bytes().unwrap(), [0, 1, 2, 3]);
        i_prot.read_field_end().unwrap();
        field(&mut i_prot, TType::Uuid, 9);
        assert_eq!(i_prot.read_uuid().unwrap(), uuid);
        i_prot.read_field_end().unwrap();

        field(&mut i_prot, TType::List, 10);
        assert_eq!(
            i_prot.read_list_begin().unwrap(),
            TListIdentifier::new(TType::Struct, 2)
        );
        for i in 0..2 {
            i_prot.read_struct_begin().unwrap();
            field(&mut i_prot, TType::I32, 1);
            assert_eq!(i_prot.read_i32().unwrap(), i);
            i_prot.read_field_end().unwrap();
            field(&mut i_prot, TType::Stop, 0);
            i_prot.read_struct_end().unwrap();
        }
        i_prot.read_list_end().unwrap();
        i_prot.read_field_end().unwrap();

        field(&mut i_prot, TType::Set, 11);
        assert_eq!(
            i_prot.read_set_begin().unwrap(),
            TSetIdentifier::new(TType::Bool, 2)
        );
        assert!(!i_prot.read_bool().unwrap());
        assert!(i_prot.read_bool().unwrap());
        i_prot.read_set_end().unwrap();
        i_prot.read_field_end().unwrap();

        field(&mut i_prot, TType::Map, 12);
        assert_eq!(
            i_prot.read_map_begin().unwrap(),
            TMapIdentifier::new(TType::I16, TType::Map, 1)
        );
        assert_eq!(i_prot.read_i16().unwrap(), -1);
        assert_eq!(
            i_prot.read_map_begin().unwrap(),
            TMapIdentifier::new(TType::Double, TType::List, 1)
        );
        assert_eq!(i_prot.read_double().unwrap(), f64::NEG_INFINITY);
        assert_eq!(
            i_prot.read_list_begin().unwrap(),
            TListIdentifier::new(TType::String, 1)
        );
        assert_eq!(i_prot.read_string().unwrap(), "x");
        i_prot.read_list_end().unwrap();
        i_prot.read_map_end().unwrap();
        i_prot.read_map_end().unwrap();
        i_prot.read_field_end().unwrap();

        field(&mut i_prot, TType::Stop, 0);
        i_prot.read_struct_end().unwrap();
        i_prot.read_message_end().unwrap();
        assert_eq!(i_prot.recursion_depth, 0);
    }

    #[test]
    fn must_read_top_level_struct_without_message() {
        let mut i_prot = input(
            r#"{"1":{"i32":10},"12":{"rec":{"1":{"tf":0},"3":{"lst":["rec",0]},"4":{"lst":["str",3,"red","green","blue"]},"5":{"tf":0},"6":{"tf":0},"7":{"tf":1}}}}"#,
        );
        i_prot.read_struct_begin().unwrap();
        let ident = i_prot.read_field_begin().unwrap();
        assert_eq!((ident.field_type, ident.id), (TType::I32, Some(1)));
        assert_eq!(i_prot.read_i32().unwrap(), 10);
        i_prot.read_field_end().unwrap();

        let ident = i_prot.read_field_begin().unwrap();
        assert_eq!((ident.field_type, ident.id), (TType::Struct, Some(12)));
        i_prot.read_struct_begin().unwrap();

        let ident = i_prot.read_field_begin().unwrap();
        assert_eq!((ident.field_type, ident.id), (TType::Bool, Some(1)));
        assert!(!i_prot.read_bool().unwrap());
        i_prot.read_field_end().unwrap();

        let ident = i_prot.read_field_begin().unwrap();
        assert_eq!((ident.field_type, ident.id), (TType::List, Some(3)));
        assert_eq!(
            i_prot.read_list_begin().unwrap(),
            TListIdentifier::new(TType::Struct, 0)
        );
        i_prot.read_list_end().unwrap();
        i_prot.read_field_end().unwrap();

        let ident = i_prot.read_field_begin().unwrap();
        assert_eq!((ident.field_type, ident.id), (TType::List, Some(4)));
        let list = i_prot.read_list_begin().unwrap();
        assert_eq!(list, TListIdentifier::new(TType::String, 3));
        let colors: Vec<String> = (0..list.size)
            .map(|_| i_prot.read_string().unwrap())
            .collect();
        assert_eq!(colors, ["red", "green", "blue"]);
        i_prot.read_list_end().unwrap();
        i_prot.read_field_end().unwrap();

        for (id, value) in [(5, false), (6, false), (7, true)] {
            let ident = i_prot.read_field_begin().unwrap();
            assert_eq!((ident.field_type, ident.id), (TType::Bool, Some(id)));
            assert_eq!(i_prot.read_bool().unwrap(), value);
            i_prot.read_field_end().unwrap();
        }
        assert_eq!(i_prot.read_field_begin().unwrap().field_type, TType::Stop);
        i_prot.read_struct_end().unwrap();
        i_prot.read_field_end().unwrap();
        assert_eq!(i_prot.read_field_begin().unwrap().field_type, TType::Stop);
        i_prot.read_struct_end().unwrap();
        assert_fully_read(&i_prot);
    }

    #[test]
    fn must_read_map_with_string_keys() {
        let mut i_prot = input(r#"["str","str",2,{"PST":"Pacific","EST":"Eastern"}]"#);
        let map = i_prot.read_map_begin().unwrap();
        assert_eq!(map, TMapIdentifier::new(TType::String, TType::String, 2));
        let entries: Vec<(String, String)> = (0..map.size)
            .map(|_| (i_prot.read_string().unwrap(), i_prot.read_string().unwrap()))
            .collect();
        assert_eq!(
            entries,
            [
                ("PST".to_owned(), "Pacific".to_owned()),
                ("EST".to_owned(), "Eastern".to_owned())
            ]
        );
        i_prot.read_map_end().unwrap();
        assert_fully_read(&i_prot);
    }

    #[test]
    fn must_read_map_with_numeric_keys() {
        let mut i_prot = input(r#"["i32","dbl",2,{"1":"NaN","-2":0.5}]"#);
        let map = i_prot.read_map_begin().unwrap();
        assert_eq!(map, TMapIdentifier::new(TType::I32, TType::Double, 2));
        assert_eq!(i_prot.read_i32().unwrap(), 1);
        assert!(i_prot.read_double().unwrap().is_nan());
        assert_eq!(i_prot.read_i32().unwrap(), -2);
        assert_eq!(i_prot.read_double().unwrap(), 0.5);
        i_prot.read_map_end().unwrap();

        let mut i_prot = input(r#"["dbl","tf",1,{"1.5":1}]"#);
        i_prot.read_map_begin().unwrap();
        assert_eq!(i_prot.read_double().unwrap(), 1.5);
        assert!(i_prot.read_bool().unwrap());
        i_prot.read_map_end().unwrap();

        assert_invalid({
            let mut i_prot = input(r#"["i32","i32",1,{1:1}]"#);
            i_prot.read_map_begin().unwrap();
            i_prot.read_i32()
        });
        assert_invalid({
            let mut i_prot = input(r#"["dbl","i32",1,{1.5:1}]"#);
            i_prot.read_map_begin().unwrap();
            i_prot.read_double()
        });
    }

    #[test]
    fn must_skip_every_type() {
        let payloads = [
            r#"{"1":{"lst":["rec",2,{"1":{"str":"app1"},"2":{"str":"Title 1"},"3":{"i64":100}},{"1":{"str":"app2"},"2":{"str":"Title 2"},"3":{"i64":200}}]}}"#,
            r#"{"1":{"str":"req-1"},"2":{"tf":1},"3":{"map":["str","str",2,{"PST":"Pacific","EST":"Eastern"}]}}"#,
            r#"{"1":{"tf":1},"2":{"i8":-1},"3":{"i16":2},"4":{"i32":3},"5":{"i64":4},"6":{"dbl":"-Infinity"},"7":{"str":"s\"}"},"8":{"uid":"00000000-0000-0000-0000-000000000000"},"9":{"set":["lst",1,["map",0]]},"10":{"map":["i64","rec",1,{"7":{"1":{"rec":{}}}}]},"11":{"map":["rec","i8",0,{}]}}"#,
            "{}",
        ];
        for payload in payloads {
            let mut i_prot = input(payload);
            assert_success!(i_prot.skip(TType::Struct));
            assert_fully_read(&i_prot);
        }
    }

    #[test]
    fn must_round_trip_messages_back_to_back() {
        let (mut i_prot, mut o_prot) = round_trip_channel();
        for seq in [1, -1] {
            let ident = TMessageIdentifier::new("m", TMessageType::Exception, seq);
            o_prot.write_message_begin(&ident).unwrap();
            o_prot
                .write_struct_begin(&TStructIdentifier::new("r"))
                .unwrap();
            o_prot.write_field_stop().unwrap();
            o_prot.write_struct_end().unwrap();
            o_prot.write_message_end().unwrap();
        }
        o_prot.transport.copy_write_buffer_to_read_buffer();
        for seq in [1, -1] {
            let ident = TMessageIdentifier::new("m", TMessageType::Exception, seq);
            assert_eq!(i_prot.read_message_begin().unwrap(), ident);
            i_prot.skip(TType::Struct).unwrap();
            i_prot.read_message_end().unwrap();
        }
        assert_eq!(i_prot.lookahead, None);
    }

    #[test]
    fn must_reject_invalid_messages() {
        assert_error_kind(
            input(r#"[2,"m",1,1,{}]"#).read_message_begin(),
            ProtocolErrorKind::BadVersion,
        );
        assert_invalid(input(r#"[1,"m",5,1,{}]"#).read_message_begin());
        assert_invalid(input(r#"[1,"m",256,1,{}]"#).read_message_begin());
        assert_invalid(input(r#"[1,"m",1,2147483648,{}]"#).read_message_begin());
        assert_invalid(input(r#"{"m",1,1,{}]"#).read_message_begin());
        assert_invalid(input(r#"[1,"m" ,1,1,{}]"#).read_message_begin());
    }

    #[test]
    fn must_reject_invalid_structs() {
        assert_error_kind(
            input(r#"{"1":{"xyz":1}}"#).skip(TType::Struct),
            ProtocolErrorKind::NotImplemented,
        );
        assert_invalid(input(r#"{"32768":{"i32":1}}"#).skip(TType::Struct));
        assert_invalid(input(r#"{1:{"i32":1}}"#).skip(TType::Struct));
        assert_invalid(input(r#"{ "1":{"i32":1}}"#).skip(TType::Struct));
        assert_invalid(input(r#"{"1":{"i32":1} }"#).skip(TType::Struct));
        assert_invalid(input(r#"{"1":{"i32":1};"2":{"i32":2}}"#).skip(TType::Struct));
        assert_invalid(input(r#"["i32",0]"#).skip(TType::Struct));
        assert!(input(r#"{"1":{"i32":1}"#).skip(TType::Struct).is_err());
    }

    #[test]
    fn must_reject_invalid_container_sizes() {
        assert_error_kind(
            input(r#"["i32",-1]"#).read_list_begin(),
            ProtocolErrorKind::NegativeSize,
        );
        assert_error_kind(
            input(r#"["i32",-3000000000]"#).read_set_begin(),
            ProtocolErrorKind::NegativeSize,
        );
        assert_error_kind(
            input(r#"["i32",3000000000]"#).read_list_begin(),
            ProtocolErrorKind::SizeLimit,
        );
        assert_error_kind(
            input(r#"["i32","i32",-1,{}]"#).read_map_begin(),
            ProtocolErrorKind::NegativeSize,
        );

        let config = TConfiguration::builder()
            .max_container_size(Some(2))
            .build()
            .unwrap();
        assert_success!(input_with_config(r#"["i32",2,1,2]"#, config.clone()).read_list_begin());
        assert_error_kind(
            input_with_config(r#"["i32",3,1,2,3]"#, config.clone()).read_list_begin(),
            ProtocolErrorKind::SizeLimit,
        );
        assert_error_kind(
            input_with_config(r#"["str","i8",3,{}]"#, config).read_map_begin(),
            ProtocolErrorKind::SizeLimit,
        );
    }

    #[test]
    fn must_enforce_read_recursion_depth_limit() {
        let config = TConfiguration::builder()
            .max_recursion_depth(Some(2))
            .build()
            .unwrap();
        let mut i_prot = input_with_config(r#"{"1":{"rec":{"1":{"rec":{}}}}}"#, config.clone());
        assert_error_kind(i_prot.skip(TType::Struct), ProtocolErrorKind::DepthLimit);

        let mut i_prot = input_with_config(r#"{"1":{"rec":{}}}"#, config);
        assert_success!(i_prot.skip(TType::Struct));
        assert_eq!(i_prot.recursion_depth, 0);
    }

    #[test]
    fn must_create_input_protocol_from_factory() {
        let factory = TJsonInputProtocolFactory::new();
        let mut i_prot = factory.create(Box::new(std::io::Cursor::new(b"[\"i8\",0]".to_vec())));
        assert_eq!(
            i_prot.read_list_begin().unwrap(),
            TListIdentifier::new(TType::I08, 0)
        );
        assert_success!(i_prot.read_list_end());
    }
}

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

#![no_main]

use thrift::TConfiguration;
use thrift::protocol::{TJsonInputProtocol, TJsonOutputProtocol, TOutputProtocol, TSerializable};
use thrift::transport::TBufferChannel;
use thrift_fuzz::fuzz_test::FuzzTest;

fn run(data: &[u8]) -> thrift::Result<()> {
    // Add some limits to optimize fuzzing
    let config = TConfiguration::builder()
        .max_message_size(Some(data.len()))
        .max_frame_size(Some(data.len()))
        .max_container_size(Some(1024))
        .max_string_size(Some(data.len()))
        .build()
        .unwrap();
    // First try to deserialize the raw input bytes
    let mut mem = TBufferChannel::with_capacity(data.len(), data.len());
    mem.set_readable_bytes(data);
    let mut protocol = TJsonInputProtocol::with_config(mem, config);
    let input = FuzzTest::read_from_in_protocol(&mut protocol)?;

    // The JSON output can be longer than the input (escapes, shorter
    // number forms), so serialize into a growable buffer
    let mut serialized = Vec::new();
    let mut out_protocol = TJsonOutputProtocol::new(&mut serialized);
    input.write_to_out_protocol(&mut out_protocol)?;
    out_protocol.flush()?;

    // Deserialize again, without the input-sized limits
    let mut in_protocol =
        TJsonInputProtocol::with_config(&serialized[..], TConfiguration::no_limits());
    let obj = FuzzTest::read_from_in_protocol(&mut in_protocol)?;

    assert_eq!(input, obj);

    Ok(())
}

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = run(data);
});

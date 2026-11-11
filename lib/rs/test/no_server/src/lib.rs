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

#[path = "../../src/base_one.rs"]
pub mod base_one;
#[path = "../../src/defaults_test.rs"]
pub mod defaults_test;

#[cfg(test)]
mod tests {
    use thrift::protocol::{TJsonInputProtocol, TJsonOutputProtocol, TSerializable};

    use super::*;

    #[test]
    fn must_use_generated_structs_without_the_server_feature() {
        let envelope = defaults_test::Envelope {
            mode: defaults_test::Mode::SECOND,
            request_id: "r1".to_owned(),
            retries: Some(2),
            ..Default::default()
        };
        let mut buf = Vec::new();
        envelope
            .write_to_out_protocol(&mut TJsonOutputProtocol::new(&mut buf))
            .unwrap();
        let read =
            defaults_test::Envelope::read_from_in_protocol(&mut TJsonInputProtocol::new(&buf[..]))
                .unwrap();
        assert_eq!(read, envelope);

        let noodle = base_one::Noodle::new("spelt".to_owned(), 110);
        let mut buf = Vec::new();
        noodle
            .write_to_out_protocol(&mut TJsonOutputProtocol::new(&mut buf))
            .unwrap();
        assert_eq!(buf, br#"{"1":{"str":"spelt"},"2":{"i64":110}}"#);
    }
}

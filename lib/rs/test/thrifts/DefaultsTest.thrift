/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// Types whose `Default` is generated even though they have required fields

enum Mode {
  First = 3,
  Second = 7,
}

// Mixes the naming styles of IDL enum values, and repeats a value
enum Action {
  OpenFile = 1,
  SUCCESS = 2,
  closeAll = 3,
  Done = 2,
}

enum Nothing {
}

typedef i32 Count

struct Inner {
  1: required string name
  2: optional i32 x
}

union Choice {
  1: string a
  2: i32 b
}

struct Envelope {
  1: required Mode mode
  2: required string requestId
  3: required Count count = 5
  4: required list<string> tags = ["a", "b"]
  5: required Inner inner
  6: Inner launch
  7: optional i64 timestamp
  8: i32 retries = 9
  9: required double ratio = 1.5
  10: required binary blob
  11: required map<i32, string> names
  12: required set<Mode> modes
}

// No Default: a union has no natural default value
struct WithUnion {
  1: required Choice choice
  2: optional i32 x
}

struct HoldsWithUnion {
  1: required WithUnion inner
}

exception Problem {
  1: required Mode mode
  2: string message
}

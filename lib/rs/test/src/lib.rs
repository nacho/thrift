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

pub mod base_one;
pub mod base_two;
pub mod defaults_test;
pub mod keywords;
pub mod midlayer;
pub mod recursive;
pub mod ultimate;

#[cfg(test)]
mod tests {

    use std::default::Default;

    use super::*;

    #[test]
    fn must_be_able_to_use_constructor() {
        let _ = midlayer::Meal::new(Some(base_one::Noodle::default()), None);
    }

    #[test]
    fn must_be_able_to_use_constructor_with_no_fields() {
        let _ = midlayer::Meal::new(None, None);
    }

    #[test]
    fn must_be_able_to_use_constructor_without_option_wrap() {
        let _ = midlayer::Meal::new(base_one::Noodle::default(), None);
    }

    #[test]
    fn must_be_able_to_use_defaults() {
        let _ = midlayer::Meal {
            noodle: Some(base_one::Noodle::default()),
            ..Default::default()
        };
    }

    #[test]
    fn unknown_union_variant_in_struct_field_is_treated_as_none() {
        use std::io::Cursor;

        use thrift::protocol::{
            TBinaryInputProtocol, TBinaryOutputProtocol, TFieldIdentifier, TOutputProtocol,
            TSerializable, TStructIdentifier, TType,
        };

        // Serialize AidKit with an unknown union variant (id=99), verify it deserializes as None.

        let mut write_buf: Vec<u8> = Vec::new();
        {
            let cursor = Cursor::new(&mut write_buf);
            let mut prot = TBinaryOutputProtocol::new(cursor, false);

            prot.write_struct_begin(&TStructIdentifier {
                name: "AidKit".to_owned(),
            })
            .unwrap();
            prot.write_field_begin(&TFieldIdentifier {
                name: None,
                field_type: TType::Struct,
                id: Some(1),
            })
            .unwrap();
            prot.write_struct_begin(&TStructIdentifier {
                name: "MeasuringAids".to_owned(),
            })
            .unwrap();
            prot.write_field_begin(&TFieldIdentifier {
                name: None,
                field_type: TType::I32,
                id: Some(99),
            })
            .unwrap();
            prot.write_i32(42).unwrap();
            prot.write_field_end().unwrap();
            prot.write_field_stop().unwrap();
            prot.write_struct_end().unwrap();
            prot.write_field_end().unwrap();
            prot.write_field_stop().unwrap();
            prot.write_struct_end().unwrap();
        }

        let read_cursor = Cursor::new(write_buf);
        let mut rprot = TBinaryInputProtocol::new(read_cursor, false);
        let kit = base_one::AidKit::read_from_in_protocol(&mut rprot)
            .expect("forward-compat deserialization should succeed");

        assert!(
            kit.aid.is_none(),
            "unknown union variant should result in None field (forward compat)"
        );
    }

    #[test]
    fn union_with_known_and_unknown_fields_deserializes_to_known_variant() {
        use std::io::Cursor;

        use thrift::protocol::{
            TBinaryInputProtocol, TBinaryOutputProtocol, TFieldIdentifier, TOutputProtocol,
            TSerializable, TStructIdentifier, TType,
        };

        // Regression test for total_field_count: a newer server may send a union
        // with a recognised variant (id=2, MeasuringCup) plus an extra unknown
        // field (id=99).  The deserializer must return the known variant rather
        // than failing with "received multiple fields for union".
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut prot = TBinaryOutputProtocol::new(Cursor::new(&mut buf), false);
            prot.write_struct_begin(&TStructIdentifier {
                name: "MeasuringAids".to_owned(),
            })
            .unwrap();
            // Known variant: MeasuringCup (field id=2)
            prot.write_field_begin(&TFieldIdentifier {
                name: None,
                field_type: TType::Struct,
                id: Some(2),
            })
            .unwrap();
            prot.write_struct_begin(&TStructIdentifier {
                name: "MeasuringCup".to_owned(),
            })
            .unwrap();
            prot.write_field_begin(&TFieldIdentifier {
                name: None,
                field_type: TType::Double,
                id: Some(1),
            })
            .unwrap();
            prot.write_double(250.0).unwrap();
            prot.write_field_end().unwrap();
            prot.write_field_stop().unwrap();
            prot.write_struct_end().unwrap();
            prot.write_field_end().unwrap();
            // Unknown field from a newer schema (field id=99)
            prot.write_field_begin(&TFieldIdentifier {
                name: None,
                field_type: TType::I32,
                id: Some(99),
            })
            .unwrap();
            prot.write_i32(0).unwrap();
            prot.write_field_end().unwrap();
            prot.write_field_stop().unwrap();
            prot.write_struct_end().unwrap();
        }

        let aids = base_one::MeasuringAids::read_from_in_protocol(&mut TBinaryInputProtocol::new(
            Cursor::new(buf),
            false,
        ))
        .expect("union with a known variant plus an unknown field must deserialize successfully");

        assert!(
            matches!(aids, base_one::MeasuringAids::Cup(_)),
            "known variant (Cup) must be returned when an unknown field is also present"
        );
    }

    #[test]
    fn nested_union_unknown_variant_does_not_corrupt_stream() {
        use std::io::Cursor;

        use thrift::protocol::{
            TBinaryInputProtocol, TBinaryOutputProtocol, TFieldIdentifier, TOutputProtocol,
            TSerializable, TStructIdentifier, TType,
        };

        // Regression test for stream corruption: when suppress_unknown catches
        // an UnknownUnionVariant error from a union field whose variant value is
        // itself a union, the outer union's Stop byte must have been consumed
        // before the error propagates.  If it hasn't, the enclosing struct loop
        // reads that orphaned 0x00 as its own Stop and silently drops every
        // subsequent field.
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut prot = TBinaryOutputProtocol::new(Cursor::new(&mut buf), false);
            // InstrumentBox
            prot.write_struct_begin(&TStructIdentifier {
                name: "InstrumentBox".to_owned(),
            })
            .unwrap();
            // field 1: InstrumentUnion
            prot.write_field_begin(&TFieldIdentifier {
                name: None,
                field_type: TType::Struct,
                id: Some(1),
            })
            .unwrap();
            // InstrumentUnion: known variant 1 whose value (MeasuringAids) carries
            // only an unknown sub-variant, so MeasuringAids::read returns
            // UnknownUnionVariant, which propagates via ? before InstrumentUnion
            // has consumed its own Stop byte.
            prot.write_struct_begin(&TStructIdentifier {
                name: "InstrumentUnion".to_owned(),
            })
            .unwrap();
            prot.write_field_begin(&TFieldIdentifier {
                name: None,
                field_type: TType::Struct,
                id: Some(1),
            })
            .unwrap();
            prot.write_struct_begin(&TStructIdentifier {
                name: "MeasuringAids".to_owned(),
            })
            .unwrap();
            prot.write_field_begin(&TFieldIdentifier {
                name: None,
                field_type: TType::I32,
                id: Some(99),
            })
            .unwrap();
            prot.write_i32(0).unwrap();
            prot.write_field_end().unwrap();
            prot.write_field_stop().unwrap();
            prot.write_struct_end().unwrap();
            prot.write_field_end().unwrap();
            prot.write_field_stop().unwrap(); // InstrumentUnion's Stop
            prot.write_struct_end().unwrap();
            prot.write_field_end().unwrap();
            // field 2: tag=42 — must survive even if field 1 triggers suppress_unknown
            prot.write_field_begin(&TFieldIdentifier {
                name: None,
                field_type: TType::I32,
                id: Some(2),
            })
            .unwrap();
            prot.write_i32(42).unwrap();
            prot.write_field_end().unwrap();
            prot.write_field_stop().unwrap();
            prot.write_struct_end().unwrap();
        }

        let ibox = base_one::InstrumentBox::read_from_in_protocol(&mut TBinaryInputProtocol::new(
            Cursor::new(buf),
            false,
        ))
        .expect("struct with a nested unknown union variant must deserialize without error");

        assert!(
            ibox.instrument.is_none(),
            "outer union with nested unknown variant must become None"
        );
        assert_eq!(
            ibox.tag,
            Some(42),
            "field following a suppressed union must not be silently dropped due to stream corruption"
        );
    }

    // ---- Rust keywords as Thrift names (THRIFT-6406) ----
    //
    // thrifts/Keywords.thrift uses names that are Rust keywords. The
    // generated code has to escape them, otherwise it does not compile.
    // Keywords of later editions become raw identifiers, the others get a
    // trailing underscore.

    #[test]
    fn keyword_field_names_are_escaped() {
        use std::io::Cursor;
        use thrift::protocol::{TBinaryInputProtocol, TBinaryOutputProtocol, TSerializable};

        let original = keywords::KeywordFields {
            r#async: Some(1),
            r#await: Some(2),
            r#dyn: Some(3),
            r#try: Some(4),
            r#gen: Some(5),
            type_: Some(6),
            match_: Some(7),
        };
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut o = TBinaryOutputProtocol::new(Cursor::new(&mut buf), false);
            original.write_to_out_protocol(&mut o).unwrap();
        }
        let read_back = keywords::KeywordFields::read_from_in_protocol(
            &mut TBinaryInputProtocol::new(Cursor::new(buf), false),
        )
        .unwrap();
        assert_eq!(original, read_back);
    }

    // Only has to compile: the client methods carry the escaped names, and
    // proc, which Rust no longer reserves, keeps its name.
    #[allow(dead_code)]
    fn keyword_method_names_are_escaped(
        client: &mut impl keywords::TKeywordMethodsSyncClient,
    ) -> thrift::Result<()> {
        client.r#gen(1, 2)?;
        client.r#try()?;
        client.type_(3)?;
        client.match_()?;
        client.r#await(4)?;
        client.proc()
    }

    // ---- Recursion depth limiting (THRIFT-6057) ----
    //
    // These exercise the generated read/write recursion guard (a thread-local
    // DepthGuard acquired at the top of every struct/union read_from_in_protocol
    // and write_to_out_protocol) through full round-trips over the recursive
    // types in test/Recursive.thrift, not by poking the guard directly.
    //
    // NB: both skip() and the struct-read guard raise ProtocolErrorKind::DepthLimit,
    // so a crafted over-limit payload must use the *known* recursive field (id 1,
    // a struct) which the generated reader matches-and-recurses into -- an unknown
    // field would be skip()-ed and trip the unrelated skip guard. The over-limit
    // assertions therefore also rely on the master baseline (no DepthGuard emitted)
    // to confirm the new guard -- not skip -- is what fires.

    const LIMIT: usize = 64;

    fn build_co_rec(depth: usize) -> recursive::CoRec {
        if depth <= 1 {
            recursive::CoRec { other: None }
        } else {
            recursive::CoRec {
                other: Some(Box::new(build_co_rec2(depth - 1))),
            }
        }
    }

    fn build_co_rec2(depth: usize) -> recursive::CoRec2 {
        if depth <= 1 {
            recursive::CoRec2 { other: None }
        } else {
            recursive::CoRec2 {
                other: Some(build_co_rec(depth - 1)),
            }
        }
    }

    fn build_co_error(depth: usize) -> recursive::CoError {
        if depth <= 1 {
            recursive::CoError { other: None }
        } else {
            recursive::CoError {
                other: Some(Box::new(build_co_error2(depth - 1))),
            }
        }
    }

    fn build_co_error2(depth: usize) -> recursive::CoError2 {
        if depth <= 1 {
            recursive::CoError2 { other: None }
        } else {
            recursive::CoError2 {
                other: Some(build_co_error(depth - 1)),
            }
        }
    }

    // Write a `depth`-deep nesting of structs that each carry the recursive
    // field (id 1, a struct) using raw protocol calls, so no write-side guard is
    // involved -- reading it back through a generated reader is what must trip
    // the read-side guard.
    fn write_nested_chain(prot: &mut dyn thrift::protocol::TOutputProtocol, depth: usize) {
        use thrift::protocol::{TFieldIdentifier, TOutputProtocol, TStructIdentifier, TType};
        prot.write_struct_begin(&TStructIdentifier {
            name: "Rec".to_owned(),
        })
        .unwrap();
        if depth > 1 {
            prot.write_field_begin(&TFieldIdentifier {
                name: None,
                field_type: TType::Struct,
                id: Some(1),
            })
            .unwrap();
            write_nested_chain(prot, depth - 1);
            prot.write_field_end().unwrap();
        }
        prot.write_field_stop().unwrap();
        prot.write_struct_end().unwrap();
    }

    fn assert_depth_limit<T: std::fmt::Debug>(result: thrift::Result<T>) {
        match result {
            Err(thrift::Error::Protocol(pe)) => assert_eq!(
                pe.kind,
                thrift::ProtocolErrorKind::DepthLimit,
                "expected DepthLimit, got {:?}",
                pe
            ),
            other => panic!("expected DepthLimit protocol error, got {:?}", other),
        }
    }

    #[test]
    fn recursion_depth_struct_round_trip_and_limit() {
        use std::io::Cursor;
        use thrift::protocol::{TBinaryInputProtocol, TBinaryOutputProtocol, TSerializable};

        // A chain exactly at the limit round-trips and is value-preserving.
        let original = build_co_rec(LIMIT);
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut o = TBinaryOutputProtocol::new(Cursor::new(&mut buf), false);
            original
                .write_to_out_protocol(&mut o)
                .expect("at-limit write must succeed");
        }
        let read_back = recursive::CoRec::read_from_in_protocol(&mut TBinaryInputProtocol::new(
            Cursor::new(buf),
            false,
        ))
        .expect("at-limit read must succeed");
        assert_eq!(original, read_back);

        // One level past the limit: the write is rejected (counter restored on drop).
        let mut o = TBinaryOutputProtocol::new(Cursor::new(Vec::new()), false);
        assert_depth_limit(build_co_rec(LIMIT + 1).write_to_out_protocol(&mut o));

        // One level past the limit: a crafted payload is rejected on read.
        let mut deep: Vec<u8> = Vec::new();
        {
            // craft with an unbounded writer so the bound is exercised only on read
            let mut o = TBinaryOutputProtocol::with_config(
                Cursor::new(&mut deep),
                false,
                thrift::TConfiguration::no_limits(),
            );
            write_nested_chain(&mut o, LIMIT + 1);
        }
        assert_depth_limit(recursive::CoRec::read_from_in_protocol(
            &mut TBinaryInputProtocol::new(Cursor::new(deep), false),
        ));
    }

    #[test]
    fn recursion_depth_exception_round_trip_and_limit() {
        use std::io::Cursor;
        use thrift::protocol::{TBinaryInputProtocol, TBinaryOutputProtocol, TSerializable};

        let original = build_co_error(LIMIT);
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut o = TBinaryOutputProtocol::new(Cursor::new(&mut buf), false);
            original
                .write_to_out_protocol(&mut o)
                .expect("at-limit write must succeed");
        }
        let read_back = recursive::CoError::read_from_in_protocol(&mut TBinaryInputProtocol::new(
            Cursor::new(buf),
            false,
        ))
        .expect("at-limit read must succeed");
        assert_eq!(original, read_back);

        let mut o = TBinaryOutputProtocol::new(Cursor::new(Vec::new()), false);
        assert_depth_limit(build_co_error(LIMIT + 1).write_to_out_protocol(&mut o));

        let mut deep: Vec<u8> = Vec::new();
        {
            // craft with an unbounded writer so the bound is exercised only on read
            let mut o = TBinaryOutputProtocol::with_config(
                Cursor::new(&mut deep),
                false,
                thrift::TConfiguration::no_limits(),
            );
            write_nested_chain(&mut o, LIMIT + 1);
        }
        assert_depth_limit(recursive::CoError::read_from_in_protocol(
            &mut TBinaryInputProtocol::new(Cursor::new(deep), false),
        ));
    }

    #[test]
    fn recursion_depth_union_read_limit() {
        use std::io::Cursor;
        use thrift::protocol::{TBinaryInputProtocol, TBinaryOutputProtocol, TSerializable};

        // CoUnion has only the recursive variant (no leaf), so a finite value
        // cannot be constructed/written; exercise the union read guard with a
        // crafted over-limit payload.
        let mut deep: Vec<u8> = Vec::new();
        {
            // craft with an unbounded writer so the bound is exercised only on read
            let mut o = TBinaryOutputProtocol::with_config(
                Cursor::new(&mut deep),
                false,
                thrift::TConfiguration::no_limits(),
            );
            write_nested_chain(&mut o, LIMIT + 1);
        }
        assert_depth_limit(recursive::CoUnion::read_from_in_protocol(
            &mut TBinaryInputProtocol::new(Cursor::new(deep), false),
        ));
    }

    // The bound lives in the protocol's struct read/write, so it applies to every
    // generated type uniformly; the binary tests above cover struct/union/exception
    // routing. This confirms the compact protocol enforces the same bound.
    #[test]
    fn recursion_depth_compact_round_trip_and_limit() {
        use std::io::Cursor;
        use thrift::protocol::{TCompactInputProtocol, TCompactOutputProtocol, TSerializable};

        let original = build_co_rec(LIMIT);
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut o = TCompactOutputProtocol::new(Cursor::new(&mut buf));
            original
                .write_to_out_protocol(&mut o)
                .expect("at-limit write must succeed");
        }
        let read_back = recursive::CoRec::read_from_in_protocol(&mut TCompactInputProtocol::new(
            Cursor::new(buf),
        ))
        .expect("at-limit read must succeed");
        assert_eq!(original, read_back);

        let mut o = TCompactOutputProtocol::new(Cursor::new(Vec::new()));
        assert_depth_limit(build_co_rec(LIMIT + 1).write_to_out_protocol(&mut o));

        let mut deep: Vec<u8> = Vec::new();
        {
            // craft with an unbounded writer so the bound is exercised only on read
            let mut o = TCompactOutputProtocol::with_config(
                Cursor::new(&mut deep),
                thrift::TConfiguration::no_limits(),
            );
            write_nested_chain(&mut o, LIMIT + 1);
        }
        assert_depth_limit(recursive::CoRec::read_from_in_protocol(
            &mut TCompactInputProtocol::new(Cursor::new(deep)),
        ));
    }

    fn to_json<T: thrift::protocol::TSerializable>(value: &T) -> String {
        let mut buf = Vec::new();
        value
            .write_to_out_protocol(&mut thrift::protocol::TJsonOutputProtocol::new(&mut buf))
            .unwrap();
        String::from_utf8(buf).unwrap()
    }

    fn from_json<T: thrift::protocol::TSerializable>(json: &str) -> thrift::Result<T> {
        let mut i_prot = thrift::protocol::TJsonInputProtocol::new(json.as_bytes());
        T::read_from_in_protocol(&mut i_prot)
    }

    fn assert_json_round_trip<T>(value: &T) -> String
    where
        T: thrift::protocol::TSerializable + PartialEq + std::fmt::Debug,
    {
        let json = to_json(value);
        assert_eq!(&from_json::<T>(&json).unwrap(), value, "{}", json);
        json
    }

    #[test]
    fn must_write_generated_structs_as_json() {
        let noodle = base_one::Noodle::new("spelt".to_owned(), 110);
        assert_eq!(
            assert_json_round_trip(&noodle),
            r#"{"1":{"str":"spelt"},"2":{"i64":110}}"#
        );
        assert_eq!(to_json(&base_one::Noodle::default()), "{}");
        assert_eq!(
            from_json::<base_one::Noodle>("{}").unwrap(),
            base_one::Noodle::new(String::new(), 0)
        );
        assert_eq!(
            assert_json_round_trip(&base_two::Ramen::new(None, 3, base_two::BrothType::SHOUYU)),
            r#"{"2":{"i32":3},"3":{"i32":1}}"#
        );
    }

    #[test]
    fn must_round_trip_generated_types_through_json() {
        use std::collections::{BTreeMap, BTreeSet};
        use thrift::OrderedFloat;

        assert_json_round_trip(&base_one::Spaghetti::new(vec![
            base_one::Noodle::new("durum".to_owned(), 100),
            base_one::Noodle::new(String::new(), 0),
        ]));
        assert_json_round_trip(&base_one::Spaghetti::new(Vec::new()));
        assert_json_round_trip(&base_one::Spaghetti::default());

        let temperatures = base_one::CookingTemperatures::new(
            BTreeSet::from([OrderedFloat(-40.0), OrderedFloat(f64::INFINITY)]),
            vec![OrderedFloat(0.1), OrderedFloat(1e300), OrderedFloat(-0.0)],
            BTreeMap::from([
                (OrderedFloat(212.0), OrderedFloat(100.0)),
                (OrderedFloat(f64::NEG_INFINITY), OrderedFloat(-1.5)),
            ]),
        );
        assert_eq!(
            assert_json_round_trip(&temperatures),
            r#"{"1":{"set":["dbl",2,-40,"Infinity"]},"2":{"lst":["dbl",3,0.1,1e300,-0]},"3":{"map":["dbl","dbl",2,{"-Infinity":-1.5,"212":100}]}}"#
        );

        let recipe = base_one::Recipe::new(
            "Soup \"of the day\"\n".to_owned(),
            "日本".to_owned(),
            -3,
            uuid::Uuid::parse_str("00112233-4455-6677-8899-aabbccddeeff").unwrap(),
        );
        assert_json_round_trip(&recipe);

        assert_json_round_trip(&base_one::CookingTools::MeasuringSpoons(BTreeSet::from([
            base_one::MeasuringSpoon::new(1),
            base_one::MeasuringSpoon::new(2),
        ])));
        assert_eq!(
            assert_json_round_trip(&base_one::CookingTools::MeasuringCups(BTreeMap::from([
                (1, "one".to_owned()),
                (-2, "minus two".to_owned()),
            ]))),
            r#"{"2":{"map":["i8","str",2,{"-2":"minus two","1":"one"}]}}"#
        );
        assert_json_round_trip(&base_one::CookingTools::Recipes(vec![recipe]));

        assert_json_round_trip(&base_one::AidKit::new(base_one::MeasuringAids::Cup(
            base_one::MeasuringCup::new(OrderedFloat(250.0)),
        )));
        assert_json_round_trip(&base_one::InstrumentBox::new(
            base_one::InstrumentUnion::MeasuringAids(base_one::MeasuringAids::Spoon(
                base_one::MeasuringSpoon::new(5),
            )),
            7,
        ));

        assert_json_round_trip(&ultimate::FullMealAndDrinks::new(
            ultimate::FullMeal::new(
                midlayer::Meal::new(
                    base_one::Noodle::new("rice".to_owned(), 90),
                    base_two::Ramen::new("tonkotsu".to_owned(), 12, base_two::BrothType::MISO),
                ),
                midlayer::Dessert::Port("ruby".to_owned()),
            ),
            ultimate::Drink::WHISKEY,
        ));
    }

    #[test]
    fn must_round_trip_recursive_types_through_json() {
        let tree = recursive::RecTree::new(
            vec![
                Box::new(recursive::RecTree::new(Vec::new(), 2)),
                Box::new(recursive::RecTree::new(
                    vec![Box::new(recursive::RecTree::new(Vec::new(), 4))],
                    3,
                )),
            ],
            1,
        );
        assert_json_round_trip(&tree);

        let list = recursive::RecList::new(
            Box::new(recursive::RecList::new(
                Box::new(recursive::RecList::new(None, 3)),
                2,
            )),
            1,
        );
        assert_json_round_trip(&recursive::VectorTest::new(vec![list.clone(), list]));

        assert_json_round_trip(&recursive::CoRec::new(Box::new(recursive::CoRec2::new(
            recursive::CoRec::new(None),
        ))));
    }

    #[test]
    fn must_skip_unknown_fields_in_json() {
        let noodle: base_one::Noodle = from_json(
            r#"{"1":{"str":"spelt"},"5":{"lst":["rec",1,{"1":{"map":["i32","str",1,{"1":"x"}]}}]},"6":{"dbl":"NaN"},"2":{"i64":110}}"#,
        )
        .unwrap();
        assert_eq!(noodle, base_one::Noodle::new("spelt".to_owned(), 110));
    }

    #[test]
    fn unknown_union_variant_in_json_struct_field_is_treated_as_none() {
        let kit: base_one::AidKit = from_json(r#"{"1":{"rec":{"99":{"i32":42}}}}"#).unwrap();
        assert!(kit.aid.is_none());

        let aids: base_one::MeasuringAids =
            from_json(r#"{"2":{"rec":{"1":{"dbl":250}}},"99":{"i32":0}}"#).unwrap();
        assert_eq!(
            aids,
            base_one::MeasuringAids::Cup(base_one::MeasuringCup::new(thrift::OrderedFloat(250.0)))
        );

        let instrument_box: base_one::InstrumentBox =
            from_json(r#"{"1":{"rec":{"1":{"rec":{"99":{"i32":42}}}}},"2":{"i32":7}}"#).unwrap();
        assert!(instrument_box.instrument.is_none());
        assert_eq!(instrument_box.tag, Some(7));
    }

    #[test]
    fn must_reject_json_missing_required_field() {
        assert!(from_json::<base_two::Ramen>(r#"{"1":{"str":"miso"}}"#).is_err());
    }

    struct RamenHandler;

    impl base_two::NapkinServiceSyncHandler for RamenHandler {
        fn handle_napkin(&self) -> thrift::Result<base_two::Napkin> {
            Ok(base_two::Napkin {})
        }
    }

    impl base_two::RamenServiceSyncHandler for RamenHandler {
        fn handle_ramen(&self, requested_noodle_count: i32) -> thrift::Result<base_two::Ramen> {
            Ok(base_two::Ramen::new(None, requested_noodle_count, None))
        }
    }

    #[test]
    fn processor_must_read_whole_json_messages() {
        use thrift::protocol::{TJsonInputProtocol, TJsonOutputProtocol};
        use thrift::server::TProcessor;

        let processor = base_two::RamenServiceSyncProcessor::new(RamenHandler);
        let requests = r#"[1,"ramen",1,1,{"1":{"i32":3}}][1,"napkin",1,2,{}]"#;
        let mut i_prot = TJsonInputProtocol::new(requests.as_bytes());
        let mut replies = Vec::new();
        {
            let mut o_prot = TJsonOutputProtocol::new(&mut replies);
            processor.process(&mut i_prot, &mut o_prot).unwrap();
            processor.process(&mut i_prot, &mut o_prot).unwrap();
        }
        assert_eq!(
            String::from_utf8(replies).unwrap(),
            r#"[1,"ramen",2,1,{"0":{"rec":{"2":{"i32":3}}}}][1,"napkin",2,2,{"0":{"rec":{}}}]"#
        );
    }

    #[test]
    fn must_default_enums_to_their_first_value() {
        assert_eq!(defaults_test::Mode::default(), defaults_test::Mode::FIRST);
    }

    #[test]
    fn must_default_structs_with_required_fields() {
        use std::collections::{BTreeMap, BTreeSet};

        let envelope = defaults_test::Envelope::default();
        assert_eq!(
            envelope,
            defaults_test::Envelope {
                mode: defaults_test::Mode::FIRST,
                request_id: String::new(),
                count: 5,
                tags: vec!["a".to_owned(), "b".to_owned()],
                inner: defaults_test::Inner {
                    name: String::new(),
                    x: None,
                },
                launch: None,
                timestamp: None,
                retries: None,
                ratio: thrift::OrderedFloat(1.5),
                blob: Vec::new(),
                names: BTreeMap::new(),
                modes: BTreeSet::new(),
            }
        );

        let launch = defaults_test::Envelope {
            mode: defaults_test::Mode::SECOND,
            request_id: "r1".to_owned(),
            launch: Some(defaults_test::Inner {
                name: "app".to_owned(),
                ..Default::default()
            }),
            retries: Some(2),
            ..Default::default()
        };
        assert_eq!(launch.count, 5);
        assert_eq!(
            to_json(&launch),
            concat!(
                r#"{"1":{"i32":7},"2":{"str":"r1"},"3":{"i32":5},"4":{"lst":["str",2,"a","b"]},"#,
                r#""5":{"rec":{"1":{"str":""}}},"6":{"rec":{"1":{"str":"app"}}},"8":{"i32":2},"#,
                r#""9":{"dbl":1.5},"#,
                r#""10":{"str":""},"11":{"map":["i32","str",0,{}]},"12":{"set":["i32",0]}}"#
            )
        );
        assert_json_round_trip(&launch);

        assert_eq!(
            defaults_test::Problem::default(),
            defaults_test::Problem {
                mode: defaults_test::Mode::FIRST,
                message: None,
            }
        );
    }
}

use std::borrow::Cow;

use encoding_rs::{ISO_8859_15, UTF_8};
use log::warn;

pub fn decode_log_msg(input: &Vec<u8>) -> (Cow<str>, &str) {
    let counter_encoding_utf8 = metrics::counter!("log_messages_encoding_utf8_count");
    let counter_encoding_iso8859_15 = metrics::counter!("log_messages_encoding_iso8859-15_count");
    let counter_encoding_unknown = metrics::counter!("log_messages_encoding_unknown_count");
    let counter_encoding_replacement = metrics::counter!("log_messages_encoding_replacement_count");

    match UTF_8.decode(&input) {
        (val, encoding, false) if encoding == UTF_8 => {
            counter_encoding_utf8.increment(1);
            (val, encoding.name())
        }
        (val, encoding, false) => {
            warn!("unknown encoding: {}", encoding.name());
            counter_encoding_unknown.increment(1);
            (val, encoding.name())
        }
        (_, _, true) => {
            let (val, encoding, was_replaced) = ISO_8859_15.decode(&input);
            if encoding == ISO_8859_15 {
                counter_encoding_iso8859_15.increment(1);
            } else {
                counter_encoding_unknown.increment(1);
                warn!("unknown encoding: {}", encoding.name());
            }
            if was_replaced {
                counter_encoding_replacement.increment(1);
            }
            (val, encoding.name())
        }
    }
}

#[cfg(test)]
mod tests {
    use encoding_rs::ISO_8859_15;
    use super::*;

    #[test]
    fn decode_iso8859_15() {
        let input = b"Wert\xe4nderung";
        let (val, encoding, was_replaced) = ISO_8859_15.decode(input);

        assert_eq!(val, "Wertänderung");
        assert_eq!(val, str::from_utf8(b"Wert\xc3\xa4nderung").unwrap()); // verify output from redis-cli
        assert_eq!(encoding.name(), "ISO-8859-15");
        assert!(!was_replaced);
    }

    #[test]
    fn parse_plc_setup() {
        let input = b"<129>1 2015-09-07T04:11:10.821 PLC_SECCPU16 - - - - Wert\xe4nderung \"SysLogDaten\".Poa_diffuse Altwert: 40,0 aktueller Wert: 44,0 CPU:SECCPU16";
        let input_vec = input.to_vec();
        let (output, encoding) = decode_log_msg(&input_vec);
        assert_eq!(encoding, "ISO-8859-15");
        assert_eq!(output, "<129>1 2015-09-07T04:11:10.821 PLC_SECCPU16 - - - - Wertänderung \"SysLogDaten\".Poa_diffuse Altwert: 40,0 aktueller Wert: 44,0 CPU:SECCPU16");
    }
}
